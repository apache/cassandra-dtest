from abc import ABCMeta, abstractproperty
from six import with_metaclass

class UpdatingMetadataWrapperBase(with_metaclass(ABCMeta, object)):
    @abstractproperty
    def _wrapped(self):
        pass

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def __getitem__(self, idx):
        return self._wrapped[idx]


class UpdatingTableMetadataWrapper(UpdatingMetadataWrapperBase):
    """
    A class that provides an interface to a table's metadata that is refreshed
    on access.
    """
    def __init__(self, cluster, ks_name, table_name, max_schema_agreement_wait=None):
        self._cluster = cluster
        self._ks_name = ks_name
        self._table_name = table_name
        self.max_schema_agreement_wait = max_schema_agreement_wait

    @property
    def _wrapped(self):
        self._cluster.refresh_table_metadata(
            self._ks_name,
            self._table_name,
            max_schema_agreement_wait=self.max_schema_agreement_wait
        )
        return self._cluster.metadata.keyspaces[self._ks_name].tables[self._table_name]

    def __repr__(self):
        return '{cls_name}(cluster={cluster}, ks_name={ks_name}, table_name={table_name}, max_schema_agreement_wait={max_wait})'.format(
            cls_name=self.__class__.__name__,
            cluster=repr(self._cluster),
            ks_name=self._ks_name,
            table_name=self._table_name,
            max_wait=self.max_schema_agreement_wait)


class UpdatingKeyspaceMetadataWrapper(UpdatingMetadataWrapperBase):
    """
    A class that provides an interface to a keyspace's metadata that is
    refreshed on access.
    """
    def __init__(self, cluster, ks_name, max_schema_agreement_wait=None):
        self._cluster = cluster
        self._ks_name = ks_name
        self.max_schema_agreement_wait = max_schema_agreement_wait

    @property
    def _wrapped(self):
        self._cluster.refresh_keyspace_metadata(
            self._ks_name,
            max_schema_agreement_wait=self.max_schema_agreement_wait
        )
        return self._cluster.metadata.keyspaces[self._ks_name]

    def __repr__(self):
        return '{cls_name}(cluster={cluster}, ks_name={ks_name}, max_schema_agreement_wait={max_wait})'.format(
            cls_name=self.__class__.__name__,
            cluster=repr(self._cluster),
            ks_name=self._ks_name,
            max_wait=self.max_schema_agreement_wait)


class UpdatingClusterMetadataWrapper(UpdatingMetadataWrapperBase):
    """
    A class that provides an interface to a cluster's metadata that is
    refreshed on access.
    """
    def __init__(self, cluster, max_schema_agreement_wait=None):
        """
        @param cluster The cassandra.cluster.Cluster object to wrap.
        """
        self._cluster = cluster
        self.max_schema_agreement_wait = max_schema_agreement_wait

    @property
    def _wrapped(self):
        self._cluster.refresh_schema_metadata(max_schema_agreement_wait=self.max_schema_agreement_wait)
        return self._cluster.metadata

    def __repr__(self):
        return '{cls_name}(cluster={cluster}, max_schema_agreement_wait={max_wait})'.format(
            cls_name=self.__class__.__name__,
            cluster=repr(self._cluster),
            max_wait=self.max_schema_agreement_wait)
