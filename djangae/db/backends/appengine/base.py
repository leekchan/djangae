#STANDARD LIB
import datetime
import decimal
import warnings
import logging

#LIBRARIES
import django
from django.conf import settings

try:
    from django.db.backends.base.operations import BaseDatabaseOperations
    from django.db.backends.base.client import BaseDatabaseClient
    from django.db.backends.base.introspection import BaseDatabaseIntrospection
    from django.db.backends.base.base import BaseDatabaseWrapper
    from django.db.backends.base.features import BaseDatabaseFeatures
    from django.db.backends.base.validation import BaseDatabaseValidation
    from django.db.backends.base.creation import BaseDatabaseCreation
    from django.db.backends.base.schema import BaseDatabaseSchemaEditor
except ImportError:
    # Django <= 1.7 (I think, at least 1.6)
    from django.db.backends import (
        BaseDatabaseOperations,
        BaseDatabaseClient,
        BaseDatabaseIntrospection,
        BaseDatabaseWrapper,
        BaseDatabaseFeatures,
        BaseDatabaseValidation
    )
    from django.db.backends.creation import BaseDatabaseCreation

    try:
        from django.db.backends.schema import BaseDatabaseSchemaEditor
    except ImportError:
        #Django < 1.7 doesn't have BaseDatabaseSchemaEditor
        class BaseDatabaseSchemaEditor(object):
            pass


from django.utils import timezone
from google.appengine.api.datastore_types import Blob, Text
from google.appengine.ext.db import metadata
from google.appengine.datastore import datastore_stub_util
from google.appengine.api import datastore, datastore_errors

#DJANGAE
from djangae.db.utils import (
    decimal_to_string,
    make_timezone_naive,
    get_datastore_key,
)
from djangae.db import caching
from djangae.db.backends.appengine.indexing import load_special_indexes
from .commands import (
    SelectCommand,
    InsertCommand,
    FlushCommand,
    UpdateCommand,
    DeleteCommand,
    coerce_unicode,
    get_field_from_column
)

from djangae.db.backends.appengine import dbapi as Database


class Connection(object):
    """ Dummy connection class """
    def __init__(self, wrapper, params):
        self.creation = wrapper.creation
        self.ops = wrapper.ops
        self.params = params
        self.queries = []

    def rollback(self):
        pass

    def commit(self):
        pass

    def close(self):
        pass


class Cursor(object):
    """ Dummy cursor class """
    def __init__(self, connection):
        self.connection = connection
        self.start_cursor = None
        self.returned_ids = []
        self.rowcount = -1
        self.last_select_command = None
        self.last_delete_command = None

    def execute(self, sql, *params):
        if isinstance(sql, SelectCommand):
            # Also catches subclasses of SelectCommand (e.g Update)
            self.last_select_command = sql
            self.rowcount = self.last_select_command.execute() or -1
        elif isinstance(sql, FlushCommand):
            sql.execute()
        elif isinstance(sql, UpdateCommand):
            self.rowcount = sql.execute()
        elif isinstance(sql, DeleteCommand):
            self.rowcount = sql.execute()
        elif isinstance(sql, InsertCommand):
            self.connection.queries.append(sql)
            self.returned_ids = sql.execute()
        else:
            raise Database.CouldBeSupportedError("Can't execute traditional SQL: '%s' (although perhaps we could make GQL work)", sql)

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def fetchone(self, delete_flag=False):
        def convert_values(value, field):
            """ For 1.7 support """
            ops = self.connection.ops

            if django.VERSION[1] < 8:
                return ops.convert_values(value, field)
            else:
                return value

        try:
            result = self.last_select_command.results.next()

            if isinstance(result, (int, long)):
                return (result,)

            query = self.last_select_command.query

            row = []

            # Prepend extra select values to the resulting row
            for col, select in query.extra_selects:
                row.append(result.get(col))

            for col in self.last_select_command.query.init_list:
                field = get_field_from_column(query.model, col)
                row.append(convert_values(result.get(col), field))

            self.returned_ids.append(result.key().id_or_name())
            return row
        except StopIteration:
            return None

    def fetchmany(self, size, delete_flag=False):
        if not self.last_select_command.results:
            return []

        result = []
        i = 0
        while i < size:
            entity = self.fetchone(delete_flag)
            if entity is None:
                break

            result.append(entity)
            i += 1

        return result

    @property
    def lastrowid(self):
        return self.returned_ids[-1].id_or_name()

    def __iter__(self):
        return self

    def close(self):
        pass

MAXINT = 9223372036854775808


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "djangae.db.backends.appengine.compiler"

    # Datastore will store all integers as 64bit long values
    integer_field_ranges = {
        'SmallIntegerField': (-MAXINT, MAXINT-1),
        'IntegerField': (-MAXINT, MAXINT-1),
        'BigIntegerField': (-MAXINT, MAXINT-1),
        'PositiveSmallIntegerField': (0, MAXINT-1),
        'PositiveIntegerField': (0, MAXINT-1),
    }

    def quote_name(self, name):
        return name

    def date_trunc_sql(self, lookup_type, field_name):
        return None

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        return '%s'

    def get_db_converters(self, expression):
        converters = super(DatabaseOperations, self).get_db_converters(expression)

        db_type = expression.field.db_type(self.connection)
        internal_type = expression.field.get_internal_type()

        if internal_type == 'TextField':
            converters.append(self.convert_textfield_value)
        elif internal_type == 'DateTimeField':
            converters.append(self.convert_datetime_value)
        elif internal_type == 'DateField':
            converters.append(self.convert_date_value)
        elif internal_type == 'TimeField':
            converters.append(self.convert_time_value)
        elif internal_type == 'DecimalField':
            converters.append(self.convert_time_value)
        elif db_type == 'list':
            converters.append(self.convert_list_value)
        elif db_type == 'set':
            converters.append(self.convert_set_value)

        return converters

    def convert_textfield_value(self, value, expression, connection, context=None):
        if isinstance(value, str):
            value = value.decode("utf-8")
        return value

    def convert_datetime_value(self, value, expression, connection, context=None):
        return self.connection.ops.value_from_db_datetime(value)

    def convert_date_value(self, value, expression, connection, context=None):
        return self.connection.ops.value_from_db_date(value)

    def convert_time_value(self, value, expression, connection, context=None):
        return self.connection.ops.value_from_db_time(value)

    def convert_decimal_value(self, value, expression, connection, context=None):
        return self.connection.ops.value_from_db_decimal(value)

    def convert_list_value(self, value, expression, connection, context=None):
        if expression.output_field.db_type(connection) != "list":
            return value

        if not value:
            value = []
        return value

    def convert_set_value(self, value, expression, connection, context=None):
        if expression.output_field.db_type(connection) != "set":
            return value

        if not value:
            value = set()
        else:
            value = set(value)
        return value


    def convert_values(self, value, field):
        """ Called when returning values from the datastore ONLY USED ON DJANGO <= 1.7"""
        value = super(DatabaseOperations, self).convert_values(value, field)

        db_type = field.db_type(self.connection)
        if db_type == 'string' and isinstance(value, str):
            value = self.convert_textfield_value(value, field, self.connection)
        elif db_type == "datetime":
            value = self.connection.ops.value_from_db_datetime(value)
        elif db_type == "date":
            value = self.connection.ops.value_from_db_date(value)
        elif db_type == "time":
            value = self.connection.ops.value_from_db_time(value)
        elif db_type == "decimal":
            value = self.connection.ops.value_from_db_decimal(value)
        elif db_type == 'list':
            if not value:
                value = [] # Convert None back to an empty list
        elif db_type == 'set':
            if not value:
                value = set()
            else:
                value = set(value)

        return value

    def sql_flush(self, style, tables, seqs, allow_cascade=False):
        return [ FlushCommand(table) for table in tables ]

    def prep_lookup_key(self, model, value, field):
        if isinstance(value, basestring):
            value = value[:500]
            left = value[500:]
            if left:
                warnings.warn("Truncating primary key that is over 500 characters. "
                              "THIS IS AN ERROR IN YOUR PROGRAM.",
                              RuntimeWarning)

            # This is a bit of a hack. Basically when you query an integer PK with a
            # string containing an int. SQL seems to return the row regardless of type, and as far as
            # I can tell, Django at no point tries to cast the value to an integer. So, in the
            # case where the internal type is an AutoField, we try to cast the string value
            # I would love a more generic solution... patches welcome!
            # It would be nice to see the SQL output of the lookup_int_as_str test is on SQL, if
            # the string is converted to an int, I'd love to know where!
            if field.get_internal_type() == 'AutoField':
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    pass

            value = get_datastore_key(model, value)
        else:
            value = get_datastore_key(model, value)

        return value

    def prep_lookup_date(self, model, value, field):
        if isinstance(value, datetime.datetime):
            return value

        return self.value_to_db_date(value)

    def prep_lookup_time(self, model, value, field):
        if isinstance(value, datetime.datetime):
            return value

        return self.value_to_db_time(value)

    def prep_lookup_value(self, model, value, field, column=None):
        if field.primary_key and (not column or column == model._meta.pk.column):
            try:
                return self.prep_lookup_key(model, value, field)
            except datastore_errors.BadValueError:
                # A key couldn't be constructed from this value
                return None

        db_type = field.db_type(self.connection)

        if db_type == 'date':
            return self.prep_lookup_date(model, value, field)
        elif db_type == 'time':
            return self.prep_lookup_time(model, value, field)
        elif db_type in ('list', 'set'):
            if hasattr(value, "__len__") and not value:
                value = None #Convert empty lists to None
            elif hasattr(value, "__iter__"):
                # Convert sets to lists
                value = list(value)

        return value

    def value_for_db(self, value, field):
        if value is None:
            return None

        db_type = field.db_type(self.connection)

        if db_type == 'string' or db_type == 'text':
            value = coerce_unicode(value)
            if db_type == 'text':
                value = Text(value)
        elif db_type == 'bytes':
            # Store BlobField, DictField and EmbeddedModelField values as Blobs.
            value = Blob(value)
        elif db_type == 'decimal':
            value = self.value_to_db_decimal(value, field.max_digits, field.decimal_places)
        elif db_type in ('list', 'set'):
            if hasattr(value, "__len__") and not value:
                value = None #Convert empty lists to None
            elif hasattr(value, "__iter__"):
                # Convert sets to lists
                value = list(value)

        return value

    def last_insert_id(self, cursor, db_table, column):
        return cursor.lastrowid

    def last_executed_query(self, cursor, sql, params):
        """
            We shouldn't have to override this, but Django's BaseOperations.last_executed_query
            assumes does u"QUERY = %r" % (sql) which blows up if you have encode unicode characters
            in your SQL. Technically this is a bug in Django for assuming that sql is ASCII but
            it's only our backend that will ever trigger the problem
        """
        return u"QUERY = {}".format(sql)

    def fetch_returned_insert_id(self, cursor):
        return cursor.lastrowid

    def value_to_db_datetime(self, value):
        value = make_timezone_naive(value)
        return value

    def value_to_db_date(self, value):
        if value is not None:
            value = datetime.datetime.combine(value, datetime.time())
        return value

    def value_to_db_time(self, value):
        if value is not None:
            value = make_timezone_naive(value)
            value = datetime.datetime.combine(datetime.datetime.fromtimestamp(0), value)
        return value

    def value_to_db_decimal(self, value, max_digits, decimal_places):
        if isinstance(value, decimal.Decimal):
            return decimal_to_string(value, max_digits, decimal_places)
        return value

    # Unlike value_to_db, these are not overridden or standard Django, it's just nice to have symmetry
    def value_from_db_datetime(self, value):
        if isinstance(value, (int, long)):
            # App Engine Query's don't return datetime fields (unlike Get) I HAVE NO IDEA WHY
            value = datetime.datetime.fromtimestamp(float(value) / 1000000.0)

        if value is not None and settings.USE_TZ and timezone.is_naive(value):
            value = value.replace(tzinfo=timezone.utc)
        return value

    def value_from_db_date(self, value):
        if isinstance(value, (int, long)):
            # App Engine Query's don't return datetime fields (unlike Get) I HAVE NO IDEA WHY
            value = datetime.datetime.fromtimestamp(float(value) / 1000000.0)

        if value:
            value = value.date()
        return value

    def value_from_db_time(self, value):
        if isinstance(value, (int, long)):
            # App Engine Query's don't return datetime fields (unlike Get) I HAVE NO IDEA WHY
            value = datetime.datetime.fromtimestamp(float(value) / 1000000.0).time()

        if value is not None and settings.USE_TZ and timezone.is_naive(value):
            value = value.replace(tzinfo=timezone.utc)

        if value:
            value = value.time()
        return value

    def value_from_db_decimal(self, value):
        if value:
            value = decimal.Decimal(value)
        return value


class DatabaseClient(BaseDatabaseClient):
    pass


class DatabaseCreation(BaseDatabaseCreation):
    data_types = {
        'AutoField':                  'key',
        'RelatedAutoField':           'key',
        'ForeignKey':                 'key',
        'OneToOneField':              'key',
        'ManyToManyField':            'key',
        'BigIntegerField':            'long',
        'BooleanField':               'bool',
        'CharField':                  'string',
        'CommaSeparatedIntegerField': 'string',
        'DateField':                  'date',
        'DateTimeField':              'datetime',
        'DecimalField':               'decimal',
        'EmailField':                 'string',
        'FileField':                  'string',
        'FilePathField':              'string',
        'FloatField':                 'float',
        'ImageField':                 'string',
        'IntegerField':               'integer',
        'IPAddressField':             'string',
        'NullBooleanField':           'bool',
        'PositiveIntegerField':       'integer',
        'PositiveSmallIntegerField':  'integer',
        'SlugField':                  'string',
        'SmallIntegerField':          'integer',
        'TimeField':                  'time',
        'URLField':                   'string',
        'TextField':                  'text',
        'XMLField':                   'text',
    }

    def __init__(self, *args, **kwargs):
        self.testbed = None
        super(DatabaseCreation, self).__init__(*args, **kwargs)

    def sql_create_model(self, model, *args, **kwargs):
        return [], {}

    def sql_for_pending_references(self, model, *args, **kwargs):
        return []

    def sql_indexes_for_model(self, model, *args, **kwargs):
        return []

    def _create_test_db(self, verbosity, autoclobber, *args):
        from google.appengine.ext import testbed # Imported lazily to prevent warnings on GAE

        assert not self.testbed

        if args:
            logging.warning("'keepdb' argument is not currently supported on the AppEngine backend")

        # We allow users to disable scattered IDs in tests. This primarily for running Django tests that
        # assume implicit ordering (yeah, annoying)
        use_scattered = not getattr(settings, "DJANGAE_SEQUENTIAL_IDS_IN_TESTS", False)

        kwargs = {
            "use_sqlite": True,
            "auto_id_policy": testbed.AUTO_ID_POLICY_SCATTERED if use_scattered else testbed.AUTO_ID_POLICY_SEQUENTIAL,
            "consistency_policy": datastore_stub_util.PseudoRandomHRConsistencyPolicy(probability=1)
        }

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub(**kwargs)
        self.testbed.init_memcache_stub()
        caching.clear_context_cache()

    def _destroy_test_db(self, name, verbosity):
        if self.testbed:
            caching.clear_context_cache()
            self.testbed.deactivate()
            self.testbed = None


class DatabaseIntrospection(BaseDatabaseIntrospection):
    @datastore.NonTransactional
    def get_table_list(self, cursor):
        kinds = metadata.get_kinds()
        try:
            from django.db.backends.base.introspection import TableInfo
            return [ TableInfo(x, "t") for x in kinds ]
        except ImportError:
            return kinds # Django <= 1.7


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    def column_sql(self, model, field):
        return "", {}

    def create_model(self, model):
        """ Don't do anything when creating tables """
        pass

    def alter_unique_together(self, *args, **kwargs):
        pass

    def alter_field(self, from_model, from_field, to_field):
        pass

    def remove_field(self, from_model, field):
        pass


class DatabaseFeatures(BaseDatabaseFeatures):
    empty_fetchmany_value = []
    supports_transactions = False  #FIXME: Make this True!
    can_return_id_from_insert = True
    supports_select_related = False
    autocommits_when_autocommit_is_off = True
    uses_savepoints = False
    allows_auto_pk_0 = False
    needs_datetime_string_cast = False


class DatabaseWrapper(BaseDatabaseWrapper):

    data_types = DatabaseCreation.data_types # These moved in 1.8

    operators = {
        'exact': '= %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s'
    }

    Database = Database

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation(self)
        self.autocommit = True

    def is_usable(self):
        return True

    def get_connection_params(self):
        return {}

    def get_new_connection(self, params):
        conn = Connection(self, params)
        load_special_indexes()  # make sure special indexes are loaded
        return conn

    def init_connection_state(self):
        pass

    def _start_transaction_under_autocommit(self):
        pass

    def _set_autocommit(self, enabled):
        self.autocommit = enabled

    def create_cursor(self):
        if not self.connection:
            self.connection = self.get_new_connection(self.settings_dict)

        return Cursor(self.connection)

    def schema_editor(self, *args, **kwargs):
        return DatabaseSchemaEditor(self, *args, **kwargs)

    def _cursor(self):
        # For < Django 1.6 compatibility
        return self.create_cursor()
