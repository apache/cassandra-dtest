import inspect
from collections import defaultdict
from unittest import TestCase

from mock import MagicMock, Mock
from tools.metadata_wrapper import (UpdatingClusterMetadataWrapper,
                                    UpdatingKeyspaceMetadataWrapper,
                                    UpdatingMetadataWrapperBase,
                                    UpdatingTableMetadataWrapper)


class UpdatingMetadataWrapperBaseTest(TestCase):
    def test_all_subclasses_known(self):
        """
        Test that all the subclasses of UpdatingMetadataWrapperBase are known
        to this test suite. Basically, this will slap us on the wrist in the
        future if we write more subclasses without writing tests for them.
        """
        self.assertEqual(
            set(UpdatingMetadataWrapperBase.__subclasses__()),
            {UpdatingTableMetadataWrapper, UpdatingKeyspaceMetadataWrapper,
             UpdatingClusterMetadataWrapper}
        )

    def _each_subclass_instantiated_with_mock_args(self):
        """
        This returns an instantiated copy of each subclass of
        UpdatingMetadataWrapperBase, passing a MagicMock for each argument on
        initialization. This lets us test properties that should hold for all
        subclasses.

        I don't like having tests depend on this weird metaprogramming, but I'd
        rather have this, which generates tests for any future subclasses, than
        miss tests for them.
        """
        for klaus in UpdatingMetadataWrapperBase.__subclasses__():
            # get length of argument list for class init
            init_arg_len = len(inspect.getargspec(klaus.__init__)[0])
            # args list is one shorter -- argspec includes self
            init_args = [MagicMock() for _ in range(init_arg_len - 1)]
            yield klaus(*init_args)

    def test_all_subclasses_defer_getattr(self):
        """
        Each subclass should defer its attribute accesses to the wrapped
        object.
        """
        for wrapper in self._each_subclass_instantiated_with_mock_args():
            self.assertIs(wrapper.foo, wrapper._wrapped.foo)

    def test_all_subclasses_defer_getitem(self):
        """
        Each subclass should defer its item accesses to the wrapped object.
        """
        for wrapper in self._each_subclass_instantiated_with_mock_args():
            # _wrapped[X] should return the same value, and should be different
            # from _wrapped[Y] for all Y
            wrapper._wrapped.__getitem__.side_effect = hash
            # check mocking correctness
            assert wrapper['foo'] != wrapper._wrapped['bar']
            assert wrapper['bar'] == wrapper._wrapped['bar']


class UpdatingTableMetadataWrapperTest(TestCase):

    def setUp(self):
        self.cluster_mock = MagicMock()
        self.ks_name_sentinel, self.table_name_sentinel = Mock(name='ks'), Mock(name='tab')
        self.max_schema_agreement_wait_sentinel = Mock(name='wait')
        self.wrapper = UpdatingTableMetadataWrapper(
            cluster=self.cluster_mock,
            ks_name=self.ks_name_sentinel,
            table_name=self.table_name_sentinel,
            max_schema_agreement_wait=self.max_schema_agreement_wait_sentinel
        )

    def test_wrapped_access_calls_refresh(self):
        """
        Accessing the wrapped object should call the table-refreshing method on
        the cluster.
        """
        self.cluster_mock.refresh_table_metadata.assert_not_called()
        self.wrapper._wrapped
        self.cluster_mock.refresh_table_metadata.assert_called_once_with(
            self.ks_name_sentinel,
            self.table_name_sentinel,
            max_schema_agreement_wait=self.max_schema_agreement_wait_sentinel
        )

    def test_default_wrapper_max_schema_agreement_wait_is_None(self):
        wrapper = UpdatingTableMetadataWrapper(
            cluster=self.cluster_mock,
            ks_name=self.ks_name_sentinel,
            table_name=self.table_name_sentinel
        )
        wrapper._wrapped
        self.cluster_mock.refresh_table_metadata.assert_called_once_with(
            self.ks_name_sentinel,
            self.table_name_sentinel,
            max_schema_agreement_wait=None
        )

    def test_wrapped_returns_table_metadata(self):
        """
        The wrapped object is accessed correctly from the internal cluster object.
        """
        # keyspaces[X] should return the same value, and should be different
        # from keyspaces[Y] for all Y. use mocks so we can do the same for
        # keyspaces[X].tables.
        keyspaces_defaultdict = defaultdict(MagicMock)
        self.cluster_mock.metadata.keyspaces.__getitem__.side_effect = lambda x: keyspaces_defaultdict[x]

        # tables[X] should return the same value, and should be different from
        # tables[Y] for all Y
        keyspaces_defaultdict[self.ks_name_sentinel].tables.__getitem__.side_effect = hash

        # check mocking correctness
        assert self.wrapper._wrapped != self.cluster_mock.metadata.keyspaces[self.ks_name_sentinel].tables['foo']

        # and this is the behavior we care about
        assert self.wrapper._wrapped ==self.cluster_mock.metadata.keyspaces[self.ks_name_sentinel].tables[self.table_name_sentinel]

    def test_repr(self):
        self.assertEqual(
            repr(self.wrapper),
            'UpdatingTableMetadataWrapper(cluster={}, ks_name={}, table_name={}, max_schema_agreement_wait={})'.format(
                self.cluster_mock, self.ks_name_sentinel,
                self.table_name_sentinel, self.max_schema_agreement_wait_sentinel
            )
        )


class UpdatingKeyspaceMetadataWrapperTest(TestCase):

    def setUp(self):
        self.cluster_mock, self.ks_name_sentinel = MagicMock(), Mock(name='ks')
        self.max_schema_agreement_wait_sentinel = Mock(name='wait')
        self.wrapper = UpdatingKeyspaceMetadataWrapper(
            cluster=self.cluster_mock, ks_name=self.ks_name_sentinel,
            max_schema_agreement_wait=self.max_schema_agreement_wait_sentinel
        )

    def test_wrapped_access_calls_refresh(self):
        """
        Accessing the wrapped object should call the keyspace-refreshing method
        on the cluster.
        """
        self.cluster_mock.refresh_keyspace_metadata.assert_not_called()
        self.wrapper._wrapped
        self.cluster_mock.refresh_keyspace_metadata.assert_called_once_with(
            self.ks_name_sentinel,
            max_schema_agreement_wait=self.max_schema_agreement_wait_sentinel
        )

    def test_default_wrapper_max_schema_agreement_wait_is_None(self):
        wrapper = UpdatingKeyspaceMetadataWrapper(
            cluster=self.cluster_mock,
            ks_name=self.ks_name_sentinel
        )
        wrapper._wrapped
        self.cluster_mock.refresh_keyspace_metadata.assert_called_once_with(
            self.ks_name_sentinel,
            max_schema_agreement_wait=None
        )

    def test_wrapped_returns_keyspace_metadata(self):
        """
        The wrapped object is accessed correctly from the internal cluster object.
        """
        # keyspaces[X] should return the same value, and should be different
        # from keyspaces[Y] for all Y
        self.cluster_mock.metadata.keyspaces.__getitem__.side_effect = hash
        # check mocking correctness
        assert self.wrapper._wrapped != self.cluster_mock.metadata.keyspaces['foo']
        assert self.wrapper._wrapped == self.cluster_mock.metadata.keyspaces[self.ks_name_sentinel]

    def test_repr(self):
        self.assertEqual(
            repr(self.wrapper),
            'UpdatingKeyspaceMetadataWrapper(cluster={}, ks_name={}, max_schema_agreement_wait={})'.format(
                self.cluster_mock, self.ks_name_sentinel, self.max_schema_agreement_wait_sentinel
            )
        )


class UpdatingClusterMetadataWrapperTest(TestCase):

    def setUp(self):
        self.cluster_mock = MagicMock()
        self.max_schema_agreement_wait_sentinel = Mock(name='wait')
        self.wrapper = UpdatingClusterMetadataWrapper(
            cluster=self.cluster_mock,
            max_schema_agreement_wait=self.max_schema_agreement_wait_sentinel
        )

    def test_wrapped_access_calls_refresh(self):
        """
        Accessing the wrapped object should call the schema-refreshing method
        on the cluster.
        """
        self.cluster_mock.refresh_schema_metadata.assert_not_called()
        self.wrapper._wrapped
        self.cluster_mock.refresh_schema_metadata.assert_called_once_with(
            max_schema_agreement_wait=self.max_schema_agreement_wait_sentinel
        )

    def test_default_wrapper_max_schema_agreement_wait_is_None(self):
        wrapper = UpdatingClusterMetadataWrapper(cluster=self.cluster_mock)
        wrapper._wrapped
        self.cluster_mock.refresh_schema_metadata.assert_called_once_with(
            max_schema_agreement_wait=None
        )

    def test_wrapped_returns_cluster_metadata(self):
        """
        The wrapped object is accessed correctly from the internal cluster object.
        """
        self.assertIs(self.wrapper._wrapped, self.cluster_mock.metadata)

    def test_repr(self):
        self.assertEqual(
            repr(self.wrapper),
            'UpdatingClusterMetadataWrapper(cluster={}, max_schema_agreement_wait={})'.format(
                self.cluster_mock,
                self.max_schema_agreement_wait_sentinel
            )
        )
