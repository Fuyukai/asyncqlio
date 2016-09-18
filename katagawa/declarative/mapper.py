from collections import MutableMapping


class _MapperHandle(object):
    """
    Represents an (unknown) field on a table.
    """
    __slots__ = ("_mapper", "_table", "_field")

    def __init__(self, mapper: 'Mapper', table: str, field: str):
        self._mapper = mapper
        self._table = table
        self._field = field

    def __call__(self, *args, **kwargs):
        return self._mapper.get_field(self._table, self._field)


class Mapper(MutableMapping):
    """
    The mapper stores a mapping of current declarative tables.

    It is used to calculate relationships between models in the mapper form.
    """

    def __init__(self):
        super().__init__()
        # Store a dict of tables.
        self.tables = {}

    def get_field(self, table: str, field: str):
        """
        Gets a field from the mapper.
        :param table: The table to fetch from.
        :param field: The field to extract.
        :return: The Column or Relationship associated with that field.
        """
        tbl = self.tables[table]
        field = tbl.__fields__[field]

        return field

    def get_field_safe(self, item: str):
        """
        Gets a field from the specified table.

        Returns a :class:`_MapperHandle` which can be called to get the underlying relationship.

        :param item: The item to get. In the format of `table.field`.
        :return: A _MapperHandle that can be used to get the field.
        """
        table, field = item.split(".", 1)

        return _MapperHandle(self, table, field)

    # Boilerplate methods
    def __len__(self):
        return len(self.tables)

    def __iter__(self):
        return self.tables.__iter__()

    def __setitem__(self, key, value):
        self.tables[key] = value

    def __getitem__(self, key):
        return self.tables[key]

    def __delitem__(self, key):
        return self.tables.pop(key)

    def __repr__(self):
        return "<Table mapper `{}`>".format(self.tables)
