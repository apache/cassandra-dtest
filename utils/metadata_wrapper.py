from abc import ABCMeta, abstractproperty


class UpdatingMetadataWrapperBase(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def _wrapped(self):
        pass

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def __getitem__(self, idx):
        return self._wrapped[idx]


class UpdatingMetadataDictWrapper(UpdatingMetadataWrapperBase):
    """
    A dictionary-like object that, given a parent implementing the
    UpdatingMetadataWrapperBase interface and the name of an attribute, updates
    its parent before checking indexes or attributes on the object at the
    attribute.
    """
    def __init__(self, parent, attr_name):
        self._parent = parent
        self._attr_name = attr_name

    @property
    def _wrapped(self):
        return getattr(self._parent._wrapped, self._attr_name)

    def __iter__(self):
        for k in self._wrapped:
            yield k

    def __repr__(self):
        return '{cls_name}(parent={parent}, attr_name={attr_name})'.format(
            cls_name=self.__class__.__name__,
            parent=repr(self._parent),
            attr_name=self._attr_name)

    def __str__(self):
        return str(self._wrapped)


class UpdatingTableMetadataWrapper(UpdatingMetadataWrapperBase):
    """
    A class that provides an interface to a table's metadata that is refreshed
    on access.
    """
    def __init__(self, cluster, ks_name, table_name):
        self._cluster = cluster
        self._ks_name = ks_name
        self._table_name = table_name

    @property
    def _wrapped(self):
        self._cluster.refresh_table_metadata(self._ks_name, self._table_name)
        return self._cluster.metadata.keyspaces[self._ks_name].tables[self._table_name]

    def __repr__(self):
        return '{cls_name}(cluster={cluster}, ks_name={ks_name}, table_name={table_name})'.format(
            cls_name=self.__class__.__name__,
            cluster=repr(self._cluster),
            ks_name=self._ks_name,
            table_name=self._table_name)


class UpdatingKeyspaceMetadataWrapper(UpdatingMetadataWrapperBase):
    """
    A class that provides an interface to a keyspace's metadata that is
    refreshed on access.
    """
    def __init__(self, cluster, ks_name):
        self._cluster = cluster
        self._ks_name = ks_name

    @property
    def _wrapped(self):
        self._cluster.refresh_keyspace_metadata(self._ks_name)
        return self._cluster.metadata.keyspaces[self._ks_name]

    def __repr__(self):
        return '{cls_name}(cluster={cluster}, ks_name={ks_name})'.format(
            cls_name=self.__class__.__name__,
            cluster=repr(self._cluster),
            ks_name=self._ks_name)


class UpdatingClusterMetadataWrapper(UpdatingMetadataWrapperBase):
    """
    A class that provides an interface to a cluster's metadata that is
    refreshed on access.
    """
    def __init__(self, cluster):
        """
        @param cluster The cassandra.cluster.Cluster object to wrap.
        """
        self._cluster = cluster

    @property
    def _wrapped(self):
        self._cluster.refresh_schema_metadata()
        return self._cluster.metadata

    def __repr__(self):
        return '{cls_name}(cluster={cluster})'.format(
            cls_name=self.__class__.__name__, cluster=repr(self._cluster))
