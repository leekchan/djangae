from django.db import models, connection
from django.dispatch import receiver
from django.db.models.signals import post_save
from django.db.models.loading import cache as model_cache

from google.appengine.api import datastore
from google.appengine.ext import deferred

from djangae.db import transaction
from djangae.fields import RelatedSetField
from djangae.contrib.mappers.pipes import MapReduceTask
from djangae.db.utils import django_instance_to_entity
from djangae.db.unique_utils import unique_identifiers_from_entity
from djangae.db.constraints import UniqueMarker


ACTION_TYPES = [
    ('check', 'Check'),  # Verify all models unique constraint markers exist and are assigned to it.
    ('repair', 'Repair'),  # Recreate any missing markers
    ('clean', 'Clean'),  # Remove any marker that isn't properly linked to an instance.
]

ACTION_STATUSES = [
    ('running', 'Running'),
    ('done', 'Done'),
]

LOG_MSGS = [
    ('missing_marker', "Marker for the unique constraint is missing"),
    ('missing_instance', "Unique constraint marker exists, but doesn't point to the instance"),
    ('already_assigned', "Marker is assigned to a different instance already"),
]

MAX_ERRORS = 100


def encode_model(model):
    return "%s,%s" % (model._meta.app_label, model._meta.model_name)

def decode_model(model_str):
    return model_cache.get_model(*model_str.split(','))


class ActionLog(models.Model):
    instance_key = models.CharField(max_length=255)
    marker_key = models.CharField(max_length=255)
    log_type = models.CharField(max_length=255, choices=LOG_MSGS)
    action = models.ForeignKey('UniqueAction')


class UniqueAction(models.Model):
    action_type = models.CharField(choices=ACTION_TYPES, max_length=100)
    model = models.CharField(max_length=100)
    status = models.CharField(choices=ACTION_STATUSES, default=ACTION_STATUSES[0][0], editable=False, max_length=100)
    logs = RelatedSetField(ActionLog, editable=False)


def _log_action(action_id, log_type, instance_key, marker_key):
    @transaction.atomic(xg=True)
    def _atomic(action_id, log_type, instance_key, marker_key):
        action = UniqueAction.objects.get(pk=action_id)
        if len(action.logs) > MAX_ERRORS:
            return

        log = ActionLog.objects.create(
            action_id=action_id,
            log_type=log_type,
            instance_key=instance_key,
            marker_key=marker_key)
        action.logs.add(log)
        action.save()
    _atomic(action_id, log_type, instance_key, marker_key)


def log(action_id, log_type, instance_key, marker_key, defer=True):
    """ Shorthand for creating an ActionLog.

    Defer doesn't accept an inline function or an atomic wrapped function directly, so
    we defer a helper function, which wraps the transactionaly decorated one. """
    if defer:
        deferred.defer(_log_action, action_id, log_type, instance_key, marker_key)
    else:
        _log_action(action_id, log_type, instance_key, marker_key)


@receiver(post_save, sender=UniqueAction)
def start_action(sender, instance, created, raw, **kwargs):
    if created == False:
        # we are saving because status is now "done"?
        return

    kwargs = dict(
        action_pk=instance.pk,
    )

    if instance.action_type == "clean":
        kwargs.update(model=instance.model)
        CleanMapper().start(**kwargs)
    else:
        kwargs.update(repair=instance.action_type=="repair")
        CheckRepairMapper(model=decode_model(instance.model)).start(**kwargs)


def _finish(*args, **kwargs):
    action_pk = kwargs.get('action_pk')

    @transaction.atomic
    def finish_the_action():
        action = UniqueAction.objects.get(pk=action_pk)
        action.status = "done"
        action.save()

    finish_the_action()


class CheckRepairMapper(MapReduceTask):
    name = 'action_mapper'

    @staticmethod
    def finish(*args, **kwargs):
        _finish(*args, **kwargs)

    @staticmethod
    def map(instance, *args, **kwargs):
        """ Figure out what markers the instance should use and verify they're attached to
        this instance. Log any weirdness and in repair mode - recreate missing markers. """
        action_id = kwargs.get("action_pk")
        repair = kwargs.get("repair")

        entity = django_instance_to_entity(connection, type(instance), instance._meta.fields, raw=False, instance=instance)
        identifiers = unique_identifiers_from_entity(type(instance), entity, ignore_pk=True)
        identifier_keys = [datastore.Key.from_path(UniqueMarker.kind(), i) for i in identifiers]

        markers = datastore.Get(identifier_keys)
        instance_key = str(entity.key())

        markers_to_save = []

        for i, m in zip(identifier_keys, markers):
            marker_key = str(i)
            if m is None:
                # Missing marker
                if repair:
                    new_marker = datastore.Entity(UniqueMarker.kind(), name=i.name())
                    new_marker['instance'] = instance_key
                    markers_to_save.append(new_marker)
                else:
                    log(action_id, "missing_marker", instance_key, marker_key)

            elif not m['instance']:
                # Marker with missing instance attribute
                if repair:
                    m['instance'] = instance_key
                    markers_to_save.append(m)
                else:
                    log(action_id, "missing_instance", instance_key, marker_key)

            elif m['instance'] != instance_key:
                # Marker already assigned to a different instance
                log(action_id, "already_assigned", instance_key, marker_key)
                # Also log in repair mode as repairing would break the other instance.

        if markers_to_save:
            datastore.Put(markers_to_save)

        yield ('_', [instance.pk])


class CleanMapper(MapReduceTask):
    name = 'action_clean_mapper'
    model = UniqueMarker

    @staticmethod
    def finish(*args, **kwargs):
        _finish(*args, **kwargs)

    @staticmethod
    def map(instance, *args, **kwargs):
        """ The Clean mapper maps over all UniqueMarker instances. """
        raise NotImplementedError
