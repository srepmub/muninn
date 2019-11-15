#
# Copyright (C) 2014-2019 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function
from muninn._compat import string_types as basestring

import copy
import datetime
import errno
import os
import re
import sys
import uuid

import muninn.config as config
import muninn.util as util

from muninn.core import Core
from muninn.exceptions import *
from muninn.extension import CascadeRule
from muninn.schema import *
from muninn.struct import Struct
from muninn import remote


class _ArchiveList(Sequence):
    _alias = "archive_list"
    sub_type = Text


class _ExtensionName(Text):
    _alias = "extension_name"

    @classmethod
    def validate(cls, value):
        super(_ExtensionName, cls).validate(value)
        if not re.match(r"[a-z][_a-z]*(\.[a-z][_a-z]*)*", value):
            raise ValueError("invalid value %r for type %r" % (value, cls.name()))


class _ExtensionList(Sequence):
    _alias = "extension_list"
    sub_type = _ExtensionName


class _ArchiveConfig(Mapping):
    _alias = "archive"

    root = Text
    backend = Text
    storage = optional(Text)
    use_symlinks = optional(Boolean)
    cascade_grace_period = optional(Integer)
    max_cascade_cycles = optional(Integer)
    external_archives = optional(_ArchiveList)
    namespace_extensions = optional(_ExtensionList)
    product_type_extensions = optional(_ExtensionList)
    remote_backend_extensions = optional(_ExtensionList)
    auth_file = optional(Text)


def _load_backend_module(name):
    module_name = "muninn.backends.%s" % name

    try:
        __import__(module_name)
    except ImportError as e:
        raise Error("import of backend %r (module %r) failed (%s)" % (name, module_name, e))

    return sys.modules[module_name]
    

def _load_storage_module(name):
    module_name = "muninn.storage.%s" % name

    try:
        __import__(module_name)
    except ImportError as e:
        raise Error("import of storage %r (module %r) failed (%s)" % (name, module_name, e))

    return sys.modules[module_name]


def _load_extension(name):
    try:
        __import__(name)
    except ImportError as e:
        raise Error("import of extension %r failed (%s)" % (name, e))

    return sys.modules[name]


def create(configuration):
    options = config.parse(configuration.get("archive", {}), _ArchiveConfig)
    _ArchiveConfig.validate(options)

    # Load and create the backend.
    backend_module = _load_backend_module(options.pop("backend"))
    backend = backend_module.create(configuration)

    # Load and create the storage backend. # TODO in storage subdir
    storage_module = _load_storage_module(options.pop("storage"))
    storage = storage_module.create(configuration)

    # Create the archive.
    namespace_extensions = options.pop("namespace_extensions", [])
    product_type_extensions = options.pop("product_type_extensions", [])
    remote_backend_extensions = options.pop("remote_backend_extensions", [])
    archive = Archive(backend=backend, storage=storage, **options)

    # Register core namespace.
    archive.register_namespace("core", Core)

    # Register custom namespaces.
    for name in namespace_extensions:
        extension = _load_extension(name)
        try:
            for namespace in extension.namespaces():
                archive.register_namespace(namespace, extension.namespace(namespace))
        except AttributeError:
            raise Error("extension %r does not implement the namespace extension API" % name)

    # Register product types.
    for name in product_type_extensions:
        extension = _load_extension(name)
        try:
            for product_type in extension.product_types():
                archive.register_product_type(product_type, extension.product_type_plugin(product_type))
        except AttributeError:
            raise Error("extension %r does not implement the product type extension API" % name)

    # Register custom remote backends.
    for name in remote_backend_extensions:
        extension = _load_extension(name)
        try:
            for remote_backend in extension.remote_backends():
                archive.register_remote_backend(remote_backend, extension.remote_backend(remote_backend))
        except AttributeError:
            raise Error("extension %r does not implement the remote backend extension API" % name)

    return archive


class Archive(object):

    def __init__(self, root, backend, storage, use_symlinks=False, cascade_grace_period=0, max_cascade_cycles=25,
                 external_archives=[], auth_file=None):
        self._root = root
        self._use_symlinks = use_symlinks
        self._cascade_grace_period = datetime.timedelta(minutes=cascade_grace_period)
        self._max_cascade_cycles = max_cascade_cycles
        self._external_archives = external_archives
        self._auth_file = auth_file

        self._namespace_schemas = {}
        self._product_type_plugins = {}
        self._remote_backend_plugins = copy.copy(remote.REMOTE_BACKENDS)
        self._export_formats = set()

        self._backend = backend
        self._backend.initialize(self._namespace_schemas)

        self._storage = storage

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _calculate_hash(self, product):
        """ calculate the hash on a product in the archive """
        product_path = self._product_path(product)
        if not product_path:
            raise Error("no data available for product '%s' (%s)" % (product.core.product_name, product.core.uuid))

        # Get the product type specific plug-in.
        plugin = self.product_type_plugin(product.core.product_type)

        # Download/symlink product in temp directory and hash
        with util.TemporaryDirectory(prefix=".calc_hash-", suffix="-%s" % product.core.uuid.hex) as tmp_path:
            use_symlinks = self._storage.supports_symlinks
            self._storage.get(product_path, tmp_path, plugin, use_symlinks=use_symlinks)

            # Determine product hash
            paths = [os.path.join(tmp_path, basename) for basename in os.listdir(tmp_path)]
            return util.product_hash(paths)

    def _catalogue_exists(self):
        return self._backend.exists()

    def _establish_invariants(self):
        repeat = True
        cycle = 0
        while repeat and cycle < self._max_cascade_cycles:
            repeat = False
            cycle += 1
            for product_type in self.product_types():
                plugin = self.product_type_plugin(product_type)

                cascade_rule = getattr(plugin, "cascade_rule", CascadeRule.IGNORE)
                if cascade_rule == CascadeRule.IGNORE:
                    continue

                strip = cascade_rule in (CascadeRule.CASCADE_PURGE_AS_STRIP, CascadeRule.STRIP)
                products = self._backend.find_products_without_source(product_type, self._cascade_grace_period, strip)
                if products:
                    repeat = True

                if strip:
                    for product in products:
                        self._strip(product)
                else:
                    for product in products:
                        self._purge(product)

                if cascade_rule in (CascadeRule.CASCADE_PURGE_AS_STRIP, CascadeRule.CASCADE_PURGE):
                    continue

                products = self._backend.find_products_without_available_source(product_type)
                if products:
                    repeat = True

                if cascade_rule in (CascadeRule.STRIP, CascadeRule.CASCADE):
                    for product in products:
                        self._strip(product)
                else:
                    for product in products:
                        self._purge(product)

    def _get_product(self, uuid):
        products = self.search('uuid == @uuid', parameters={'uuid': uuid})
        if len(products) == 0:
            raise Error('No product found with UUID: %s' % uuid)
        assert len(products) == 1
        return products[0]

    def _product_path(self, product):
        if getattr(product.core, "archive_path", None) is None:
            return None

        return self._storage.product_path(product)

    def _purge(self, product):
        # Remove the product from the product catalogue.
        self._backend.delete_product_properties(product.core.uuid)

        # Remove any data on disk associated with the product.
        self._remove(product)

    def _relocate(self, product, properties=None):
        """Relocate a product to the archive_path reported by the product type plugin.
        Returns the new archive_path if the product was moved."""
        result = None
        product_archive_path = product.core.archive_path
        product_path = self._product_path(product)
        if properties:
            product = copy.deepcopy(product)
            product.update(properties)
        plugin = self.product_type_plugin(product.core.product_type)
        plugin_archive_path = plugin.archive_path(product)

        if product_archive_path != plugin_archive_path:
            abs_archive_path = os.path.realpath(os.path.join(self._root, plugin_archive_path))
            util.make_path(abs_archive_path)
            os.rename(product_path, os.path.join(abs_archive_path, product.core.physical_name))
            result = plugin_archive_path

        return result

    def _remove(self, product):
        # If the product has no data on disk associated with it, return.
        product_path = self._product_path(product)
        if product_path is None:
            # If the product does not exist, do not consider this an error.
            return

        # Remove the data associated with the product from disk.
        plugin = self.product_type_plugin(product.core.product_type)
        self._storage.delete(product_path, product, plugin)

    def _retrieve(self, product, target_path, use_symlinks=False):
        # Determine the path of the product on disk.
        product_path = self._product_path(product)
        if not product_path:
            raise Error("no data available for product '%s' (%s)" % (product.core.product_name, product.core.uuid))

        # Get the product type specific plug-in.
        plugin = self.product_type_plugin(product.core.product_type)

        # Symbolic link or copy the product at or to the specified target directory.
        self._storage.get(product_path, target_path, plugin, use_symlinks)

        return os.path.join(target_path, os.path.basename(product_path))

    def _strip(self, product):
        # Set the archive path to None to indicate the product has no data on disk associated with it.
        self.update_properties(Struct({'core': {'active': True, 'archive_path': None, 'archive_date': None}}),
                               product.core.uuid)

        # Remove any data on disk associated with the product.
        self._remove(product)

    def _update_export_formats(self, plugin):
        # Find all callables of which the name starts with "export_". The remainder of the name is used as the name of
        # the export format.
        for export_method_name in dir(plugin):
            if not export_method_name.startswith("export_"):
                continue

            _, _, export_format = export_method_name.partition("_")
            if not export_format:
                continue

            export_method = getattr(plugin, export_method_name)
            if export_method is None or not hasattr(export_method, "__call__"):
                continue

            self._export_formats.add(export_format)

    def _update_metadata_date(self, properties):
        """ add a core.metadata_date field if it did not yet exist and set it to the current date """
        if "core" not in properties:
            properties.core = Struct()
        properties.core.metadata_date = self._backend.server_time_utc()

    def auth_file(self):
        """Return the path of the authentication file to download from remote locations"""
        return self._auth_file

    def close(self):
        """Close the archive immediately instead of when (and if) the archive instance is collected.

        Using the archive after calling this function results in undefined behavior.

        """
        self._backend.disconnect()

    def count(self, where="", parameters={}):
        """Return the number of products matching the specified search expression.

        Keyword arguments:
        where       --  Search expression.
        parameters  --  Parameters referenced in the search expression (if any).

        """
        return self._backend.count(where, parameters)

    def create_properties(self, properties):
        """ Create record for product in the product catalogue.
            An important side effect of this operation is that it
            will fail if:

            1. The core.uuid is not unique within the product catalogue.
            2. The combination of core.archive_path and core.physical_name is
               not unique within the product catalogue.
        """
        self._update_metadata_date(properties)
        self._backend.insert_product_properties(properties)

    def derived_products(self, uuid):
        """Return the UUIDs of the products that are linked to the given product as derived products."""
        return self._backend.derived_products(uuid)

    def destroy(self):
        """Completely remove the archive, both the products as well as the product catalogue.

        Using the archive after calling this function results in undefined behavior. The prepare() function can be used
        to bring the archive back into a useable state.

        """
        self.destroy_catalogue()

        self._storage.destroy()

    def destroy_catalogue(self):
        """Completely remove the catalogue database, but leaving the datastore on disk untouched.

        Using the archive after calling this function results in undefined behavior.
        Using the prepare_catalogue() function and ingesting all products again, can bring the archive
        back into a useable state.

        """
        # Call the backend to remove anything related to the archive.
        if self._catalogue_exists():
            self._backend.destroy()

    def export(self, where="", parameters={}, target_path=os.path.curdir, format=None):
        """Export one or more products from the archive. Return the list of file paths of the exported products.

        By default, a copy of the original product will be retrieved from the archive. This default behavior can be
        customized by the product type plug-in. For example, the custom implementation for a certain product type might
        retrieve one or more derived products and bundle them together with the product itself.

        Keyword arguments:
        where           --  Search expression that determines which products to export.
        parameters      --  Parameters referenced in the search expression (if any).
        target_path     --  Directory in which the retrieved products will be stored.
        format          --  Format in which the products will be exported.

        """
        export_method_name = "export"
        if format is not None:
            if re.match("[a-zA-Z]\\w*$", format) is None:
                raise Error("invalid export format '%s'" % format)
            export_method_name = export_method_name + "_" + format

        result = []
        products = self.search(where=where, parameters=parameters,
                               property_names=['uuid', 'active', 'product_type', 'product_name', 'archive_path',
                                               'physical_name'])
        for product in products:
            if not product.core.active:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

            # Use the format specific export_<format>() method from the product type plug-in, or the default export()
            # method if no format was specified. Call self._retrieve() as a fall back if the product type plug-in does
            # not define its own export() method.
            #
            # Note the use of getattr() / hasattr() instead of a try + except AttributeError block, to avoid hiding
            # AttributeError instances raised by the plug-in.
            plugin = self.product_type_plugin(product.core.product_type)

            export_method = getattr(plugin, export_method_name, None)
            if export_method is not None:
                exported_path = export_method(self, product, target_path)
            elif format is not None:
                raise Error("export format '%s' not supported for product '%s' (%s)" %
                            (format, product.core.product_name, product.core.uuid))
            else:
                exported_path = self._retrieve(product, target_path, False)
            result.append(exported_path)

        return result

    def export_by_name(self, product_name, target_path=os.path.curdir, format=None):
        """Export one or more products from the archive by name.

        This is a convenience function that is equivalent to:

            self.export("product_name == @product_name", {"product_name": product_name}, target_path, format)

        NB. A product name is not guaranteed to be unique (only the combination of product type and product name is),
        so this function may export one or more products.

        An exception will be raised if no products with the specified name can be found.

        """
        paths = self.export("product_name == @product_name", {"product_name": product_name}, target_path, format)
        if not paths:
            raise Error("no products found with name '%s'" % product_name)
        return paths

    def export_by_uuid(self, uuid, target_path=os.path.curdir, format=None):
        """Export a product from the archive by uuid.

        This is a convenience function that is equivalent to:

            self.export("uuid == @uuid", {"uuid": uuid}, target_path, format)

        An exception will be raised if no product with the specified uuid can be found.

        """
        paths = self.export("uuid == @uuid", {"uuid": uuid}, target_path, format)
        if not paths:
            raise Error("product with uuid '%s' not found" % uuid)
        return paths

    def export_formats(self):
        """Return a list of supported alternative export formats."""
        return list(self._export_formats)

    def external_archives(self):
        """Return the identifiers of any archives that are configured as external archives associated to this archive.

        External archives contain products which are linked to by products in this archive, but which are not stored in
        this archive themselves.

        """
        return self._external_archives

    @staticmethod
    def generate_uuid():
        """Return a new generated UUID that can be used as UUID for a product metadata record"""
        return uuid.uuid4()

    def identify(self, paths):
        """Determine the product type of the product (specified as a single path, or a list of paths if it is a
        multi-part product).

        """
        for product_type, plugin in self._product_type_plugins.items():
            if plugin.identify(paths):
                return product_type

        raise Error("unable to identify product: \"%s\"" % paths)

    def ingest(self, paths, product_type=None, properties=None, ingest_product=True, use_symlinks=None,
               verify_hash=False, use_current_path=False, force=False):
        """Ingest a product into the archive. Multiple paths can be specified, but the set of files and/or directories
        these paths refer to is always ingested as a single logical product.

        Product ingestion consists of two steps. First, product properties are extracted from the product and are used
        to create an entry for the product in the product catalogue. Second, the product itself is ingested, either by
        copying the product or by creating symbolic links to the product.

        If the product to be ingested is already located at the target location within the archive (and there was not
        already another catalogue entry pointing to it), muninn will leave the product at its location as-is, and won't
        try to copy/symlink it.

        Keyword arguments:
        product_type     -- Product type of the product to ingest. If left unspecified, an attempt will be made to
                            determine the product type automatically. By default, the product type will be determined
                            automatically.
        properties       -- Used as product properties if specified. No properties will be extracted from the product
                            in this case.
        ingest_product   -- If set to False, the product itself will not be ingested into the archive, only its
                            properties. By default, the product will be ingested.
        use_symlinks     -- If set to True, symbolic links to the original product will be stored in the archive
                            instead of a copy of the original product. If set to None, the value of the corresponding
                            archive wide configuration option will be used. By default, the archive configuration will
                            be used.
                            This option is ignored if use_current_path=True.
        verify_hash      -- If set to True then, after the ingestion, the product in the archive will be matched against
                            the hash from the metadata (only if the metadata contained a hash).
        use_current_path -- Ingest the product by keeping the file(s) at the current path (which must be inside the
                            root directory of the archive).
                            This option is ignored if ingest_product=False.
        force            -- If set to True then any existing product with the same type and name (unique constraint)
                            will be removed before ingestion, including partially ingested products.
                            NB. Depending on product type specific cascade rules, removing a product can result in one
                            or more derived products being removed (or stripped) along with it.

        """
        if isinstance(paths, basestring):
            paths = [paths]

        if not paths:
            raise Error("nothing to ingest")

        if not self._storage.exists():
            raise Error("archive root path '%s' does not exist" % self._root)

        # Use absolute paths to make error messages more useful, and to avoid broken links when ingesting a product
        # using symbolic links.
        paths = [os.path.realpath(path) for path in paths]

        # Ensure that the set of files and / or directories that make up the product does not contain duplicate
        # basenames.
        if util.contains_duplicates([os.path.basename(path) for path in paths]):
            raise Error("basename of each part should be unique for multi-part products")

        # Get the product type plug-in.
        if product_type is None:
            product_type = self.identify(paths)
        plugin = self.product_type_plugin(product_type)

        # Extract product metadata.
        if properties is None:
            metadata = plugin.analyze(paths)
            if isinstance(metadata, (tuple, list)):
                properties, tags = metadata
            else:
                properties, tags = metadata, []
        else:
            properties, tags = copy.deepcopy(properties), []

        assert properties is not None and "core" in properties
        assert "product_name" in properties.core and properties.core.product_name, \
            "product_name is required in core.properties"

        # Set core product properties that are not determined by the plugin.
        # Note that metadata_date is set automatically by create_properties()
        # and archive_date is properly set when we activate the product.
        properties.core.uuid = self.generate_uuid()
        properties.core.active = False
        properties.core.hash = None
        properties.core.size = util.product_size(paths)
        properties.core.metadata_date = None
        properties.core.archive_date = None
        properties.core.archive_path = None
        properties.core.product_type = product_type
        properties.core.physical_name = None

        # Determine physical product name.
        if plugin.use_enclosing_directory:
            properties.core.physical_name = plugin.enclosing_directory(properties)
        elif len(paths) == 1:
            properties.core.physical_name = os.path.basename(paths[0])
        else:
            raise Error("cannot determine physical name for multi-part product")

        # Remove existing product with the same product type and name before ingesting
        if force:
            self.remove(where="core.product_type == @product_type and core.product_name == @product_name",
                        parameters={'product_type': properties.core.product_type,
                                    'product_name': properties.core.product_name}, force=True)

        self.create_properties(properties)

        # Try to determine the product hash and ingest the product into the archive.
        try:
            # Determine product hash. Since it is an expensive operation, the hash is computed after inserting the
            # product properties so we won't needlessly compute it for products that fail ingestion into the catalogue.
            if plugin.use_hash:
                try:
                    properties.core.hash = util.product_hash(paths)
                except EnvironmentError as _error:
                    raise Error("cannot determine product hash [%s]" % (_error,))
                # Update the product hash in the product catalogue.
                self.update_properties(Struct({'core': {'hash': properties.core.hash}}), properties.core.uuid)

            if ingest_product:
                use_symlinks = (use_symlinks or use_symlinks is None and self._use_symlinks)
                self._storage.put(paths, properties, plugin, use_current_path, use_symlinks)

                metadata = {'archive_path': properties.core.archive_path}
                self.update_properties(Struct({'core': metadata}), properties.core.uuid)

                # Verify product hash after copy
                if verify_hash:
                    if self.verify_hash("uuid == @uuid", {"uuid": properties.core.uuid}):
                        raise Error("ingested product has incorrect hash")

                properties.core.archive_date = self._backend.server_time_utc()
        except:
            # Try to remove the entry for this product from the product catalogue.
            self._backend.delete_product_properties(properties.core.uuid)
            raise

        # Activate product.
        properties.core.active = True
        metadata = {
            'active': properties.core.active,
            'archive_date': properties.core.archive_date,
        }
        self.update_properties(Struct({'core': metadata}), properties.core.uuid)

        # Set product tags.
        self._backend.tag(properties.core.uuid, tags)

        # Run the post ingest hook (if defined by the product type plug-in).
        #
        # Note that hasattr() is used instead of a try + except block that swallows AttributeError to avoid hiding
        # AttributeError instances raised by the plug-in.
        if hasattr(plugin, "post_ingest_hook"):
            plugin.post_ingest_hook(self, properties)

        return properties

    def link(self, uuid_, source_uuids):
        """Link a product to one or more source products."""
        if isinstance(source_uuids, uuid.UUID):
            source_uuids = [source_uuids]

        self._backend.link(uuid_, source_uuids)

    def namespace_schema(self, namespace):
        """Return the schema definition of a namespace."""
        try:
            return self._namespace_schemas[namespace]
        except KeyError:
            raise Error("undefined namespace: \"%s\"; defined namespaces: %s" %
                        (namespace, util.quoted_list(self._namespace_schemas.keys())))

    def namespaces(self):
        """Return a list of supported namespaces."""
        return list(self._namespace_schemas.keys())

    def prepare(self, force=False):
        """Prepare the archive for (first) use.

        The root path will be created and the product catalogue will be initialized such that the archive is ready for
        use.

        Keyword arguments:
        force   --  If set to True then any existing products and / or product catalogue will be removed.

        """
        if not force:
            if self._storage.exists():
                raise Error("archive directory already exists")
            if self._catalogue_exists():
                raise Error("catalogue already exists")

        # Remove anything related to the archive.
        self.destroy()

        # Prepare the archive for use.
        self._backend.prepare()
        self._storage.prepare()

    def prepare_catalogue(self, dry_run=False):
        """Prepare the catalogue of the archive for (first) use.

        """
        return self._backend.prepare(dry_run=dry_run)

    def product_path(self, uuid_or_name_or_properties):
        """Return the path on disk where the product with the specified product.
        Product can be specified by either: uuid, product name or product properties.
        """
        if isinstance(uuid_or_name_or_properties, Struct):
            product = uuid_or_name_or_properties
        elif isinstance(uuid_or_name_or_properties, uuid.UUID):
            products = self.search(where="uuid == @uuid", parameters={"uuid": uuid_or_name_or_properties},
                                   property_names=['archive_path', 'physical_name'])
            if len(products) == 0:
                raise Error("product with uuid '%s' not found" % uuid_or_name_or_properties)
            assert len(products) == 1
            product = products[0]
        else:
            products = self.search(where="product_name == @product_name",
                                   parameters={"product_name": uuid_or_name_or_properties},
                                   property_names=['archive_path', 'physical_name'])
            if len(products) == 0:
                raise Error("product with name '%s' not found" % uuid_or_name_or_properties)
            if len(products) != 1:
                raise Error("more than one product found with name '%s'" % uuid_or_name_or_properties)
            product = products[0]

        return self._product_path(product)

    def product_type_plugin(self, product_type):
        """Return a reference to the product type plugin for a product type."""
        try:
            return self._product_type_plugins[product_type]
        except KeyError:
            raise Error("undefined product type: \"%s\"; defined product types: %s" % (product_type, util.quoted_list(self._product_type_plugins.keys())))

    def product_types(self):
        """Return a list of supported product types."""
        return list(self._product_type_plugins.keys())

    def pull(self, where="", parameters={}, verify_hash=False):
        """Pull one or more remote products into the archive.
        Return the number of products pulled.

        Products should have a valid remote_url core metadata field and they should not yet exist in the local
        archive (i.e. the archive_path core metadata field should not be set).

        Keyword arguments:
        where         --  Search expression.
        parameters    --  Parameters referenced in the search expression (if any).
        verify_hash   --  If set to True then, after the pull, the product in the archive will be matched against
                          the hash from the metadata (only if the metadata contained a hash).

        """
        queue = self.search(where=where, parameters=parameters)
        for product in queue:
            if not product.core.active:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))
            if 'archive_path' in product.core:
                raise Error("product '%s' (%s) is already in the local archive" %
                            (product.core.product_name, product.core.uuid))
            if 'remote_url' not in product.core:
                raise Error("product '%s' (%s) does not have a remote_url" %
                            (product.core.product_name, product.core.uuid))

            plugin = self.product_type_plugin(product.core.product_type)
            product.core.archive_path = plugin.archive_path(product)

            # set archive_path and deactivate while we pull it in
            metadata = {'active': False, 'archive_path': product.core.archive_path}
            self.update_properties(Struct({'core': metadata}), product.core.uuid)

            # pull product
            try:
                remote.pull(self, product)
            except:
                # reset active/archive_path values
                metadata = {'active': True, 'archive_path': None}
                self.update_properties(Struct({'core': metadata}), product.core.uuid)
                raise

            # reactivate and update size
            size = 0 # util.product_size(self._product_path(product)) TODO
            metadata = {'active': True, 'archive_date': self._backend.server_time_utc(), 'size': size}
            self.update_properties(Struct({'core': metadata}), product.core.uuid)

            # verify product hash.
            if verify_hash and 'hash' in product.core:
                if self.verify_hash("uuid == @uuid", {"uuid": product.core.uuid}):
                    raise Error("pulled product '%s' (%s) has incorrect hash" %
                                (product.core.product_name, product.core.uuid))

            # Run the post pull hook (if defined by the product type plug-in).
            if hasattr(plugin, "post_pull_hook"):
                plugin.post_pull_hook(self, product)

        return len(queue)

    def rebuild_properties(self, uuid, disable_hooks=False, use_current_path=False):
        """Rebuilds product properties by re-extracting these properties (using product type plug-ins) from the
        products stored in the archive.
        Only properties and tags that are returned by the product type plug-in will be updated. Other properties or
        tags will remain as they were.

        Keyword arguments:
        disable_hooks --  Disable product type hooks (not meant for routine operation).
        use_current_path -- Do not attempt to relocate the product to the location specified in the product
                            type plug-in. Useful for read-only archives.

        """
        restricted_properties = set(["uuid", "active", "hash", "size", "metadata_date", "archive_date", "archive_path",
                                     "product_type", "physical_name"])

        product = self._get_product(uuid)
        if not product.core.active:
            raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

        # Determine the path of the product on disk.
        product_path = self._product_path(product)
        if not product_path:
            raise Error("no data available for product '%s' (%s)" % (product.core.product_name, product.core.uuid))

        # Extract product metadata.
        plugin = self.product_type_plugin(product.core.product_type)

        if plugin.use_enclosing_directory:
            paths = [os.path.join(product_path, basename) for basename in os.listdir(product_path)]
        else:
            paths = [product_path]
        metadata = plugin.analyze(paths)

        if isinstance(metadata, (tuple, list)):
            properties, tags = metadata
        else:
            properties, tags = metadata, []

        # Remove properties that should not be changed.
        assert "core" in properties
        for name in restricted_properties:
            try:
                delattr(properties.core, name)
            except AttributeError:
                pass

        # update size
        properties.core.size = util.product_size(self._product_path(product))

        # Make sure product is stored in the correct location
        if not use_current_path:
            new_archive_path = self._relocate(product, properties)
            if new_archive_path:
                properties.core.archive_path = new_archive_path

        # Update product properties.
        self.update_properties(properties, uuid=product.core.uuid, create_namespaces=True)

        # Update tags.
        self.tag(product.core.uuid, tags)

        # Run the post ingest hook (if defined by the product type plug-in).
        #
        # Note that hasattr() is used instead of a try + except block that swallows AttributeError to avoid hiding
        # AttributeError instances raised by the plug-in.
        if not disable_hooks and hasattr(plugin, "post_ingest_hook"):
            product.update(properties)
            if 'hash' not in product.core:
                product.core.hash = None
            plugin.post_ingest_hook(self, product)

    def rebuild_pull_properties(self, uuid, verify_hash=False, disable_hooks=False, use_current_path=False):
        """Refresh products by re-running the pull, but using the existing products stored in the archive.

        Keyword arguments:
        verify_hash   --  If set to True then the product in the archive will be matched against
                          the hash from the metadata (only if the metadata contained a hash).
        disable_hooks --  Disable product type hooks (not meant for routine operation).
        use_current_path -- Do not attempt to relocate the product to the location specified in the product
                            type plug-in. Useful for read-only archives.

        """
        product = self._get_product(uuid)
        if 'archive_path' not in product.core:
            raise Error("cannot update missing product")
        if 'remote_url' not in product.core:
            raise Error("cannot pull products that have no remote_url")

        plugin = self.product_type_plugin(product.core.product_type)

        # make sure product is stored in the correct location
        if not use_current_path:
            new_archive_path = self._relocate(product)
            if new_archive_path:
                metadata = {'archive_path': new_archive_path}
                self.update_properties(Struct({'core': metadata}), product.core.uuid)
                product.core.archive_path = new_archive_path

        # update size
        product.core.size = util.product_size(self._product_path(product))

        # verify product hash.
        if verify_hash and 'hash' in product.core:
            if self.verify_hash("uuid == @uuid", {"uuid": product.core.uuid}):
                raise Error("pulled product '%s' (%s) has incorrect hash" %
                            (product.core.product_name, product.core.uuid))

        # Run the post pull hook (if defined by the product type plug-in).
        if not disable_hooks and hasattr(plugin, "post_pull_hook"):
            plugin.post_pull_hook(self, product)

    def register_namespace(self, namespace, schema):
        """Register a namespace.

        Arguments:
        namespace -- Name of the namespace.
        schema    -- Schema definition of the namespace.

        """
        if not re.match(r"[a-z][_a-z]*(\.[a-z][_a-z]*)*", namespace):
            raise ValueError("invalid namespace name %s" % namespace)
        if namespace in self._namespace_schemas:
            raise Error("redefinition of namespace: \"%s\"" % namespace)

        self._namespace_schemas[namespace] = schema

    def register_product_type(self, product_type, plugin):
        """Register a product type.

        Arguments:
        product_type -- Product type.
        plugin       -- Reference to an object that implements the product type plugin API and as such takes care of
                        the details of extracting product properties from products of the specified product type.

        """
        if product_type in self._product_type_plugins:
            raise Error("redefinition of product type: \"%s\"" % product_type)

        # Quick verify of the plugin interface
        for attr in ['use_enclosing_directory', 'use_hash']:
            if not hasattr(plugin, attr):
                raise Error("missing '%s' attribute in plugin for product type \"%s\"" % (attr, product_type))
        methods = ['identify', 'analyze', 'archive_path']
        if plugin.use_enclosing_directory:
            methods += ['enclosing_directory']
        for method in methods:
            if not hasattr(plugin, method):
                raise Error("missing '%s' method in plugin for product type \"%s\"" % (method, product_type))

        self._product_type_plugins[product_type] = plugin
        self._update_export_formats(plugin)

    def register_remote_backend(self, remote_backend, plugin):
        """Register a remote backend.

        Arguments:
        remote_backend -- Remote backend.
        plugin         -- Reference to an object that implements the remote backend plugin API and as such takes care of
                          the details of extracting product properties from products of the specified remote backend.

        """
        if remote_backend in self._remote_backend_plugins:
            raise Error("redefinition of remote backend: \"%s\"" % remote_backend)

        self._remote_backend_plugins[remote_backend] = plugin

    def remote_backend(self, remote_backend):
        """Return the schema definition of a remote_backend."""
        try:
            return self._remote_backend_plugins[remote_backend]
        except KeyError:
            raise Error("undefined remote backend: \"%s\"; defined remote backends: %s" % (remote_backend, util.quoted_list(self._remote_backend.keys())))

    def remote_backends(self):
        """Return a list of supported remote_backends."""
        return list(self._remote_backend_plugins.keys())

    def remove(self, where="", parameters={}, force=False):
        """Remove one or more products from the archive, both from disk as well as from the product catalogue. Return
        the number of products removed.

        NB. Depending on product type specific cascade rules, removing a product can result in one or more derived
        products being removed (or stripped) along with it. Such products are _not_ included in the returned count.

        Keyword arguments:
        where       --  Search expression that determines which products to remove.
        parameters  --  Parameters referenced in the search expression (if any).
        force       --  If set to True, also remove partially ingested products. This affects products for which a
                        failure occured during ingestion, as well as products in the process of being ingested. Use
                        this option with care.
        """
        products = self.search(where=where, parameters=parameters,
                               property_names=['uuid', 'active', 'product_name', 'archive_path', 'physical_name', 'product_type'])
        for product in products:
            if not product.core.active and not force:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

            self._purge(product)

        # Remove (or strip) derived products if necessary.
        if len(products) > 0:
            self._establish_invariants()

        return len(products)

    def remove_by_name(self, product_name, force=False):
        """Remove one or more products from the archive by name.

        This is a convenience function that is equivalent to:

            self.remove("product_name == @product_name", {"product_name": product_name}, force)

        NB. A product name is not guaranteed to be unique (only the combination of product type and product name is),
        so this function may remove one or more products.

        An exception will be raised if no products with the specified name can be found.

        """
        count = self.remove("product_name == @product_name", {"product_name": product_name}, force)
        if count == 0:
            raise Error("no products found with name '%s'" % product_name)
        return count

    def remove_by_uuid(self, uuid, force=False):
        """Remove a product from the archive by uuid.

        This is a convenience function that is equivalent to:

            self.remove("uuid == @uuid", {"uuid": uuid}, force)

        An exception will be raised if no product with the specified uuid can be found.

        """
        count = self.remove("uuid == @uuid", {"uuid": uuid}, force)
        if count == 0:
            raise Error("product with uuid '%s' not found" % uuid)
        return count

    def retrieve(self, where="", parameters={}, target_path=os.path.curdir, use_symlinks=False):
        """Retrieve one or more products from the archive. Return the number of products retrieved.

        Keyword arguments:
        where           --  Search expression that determines which products to retrieve.
        parameters      --  Parameters referenced in the search expression (if any).
        target_path     --  Directory under which the retrieved products will be stored.
        use_symlinks    --  If set to True, products will be retrieved as symbolic links to the original products kept
                            in the archive. If set to False, products will retrieved as copies of the original products.
                            By default, products will be retrieved as copies.

        """
        products = self.search(where=where, parameters=parameters,
                               property_names=['uuid', 'active', 'product_type', 'product_name', 'archive_path',
                                               'physical_name'])
        for product in products:
            if not product.core.active or 'archive_path' not in product.core:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

            self._retrieve(product, target_path, use_symlinks)

        return len(products)

    def retrieve_by_name(self, product_name, target_path=os.path.curdir, use_symlinks=False):
        """Retrieve a product from the archive by name.

        This is a convenience function that is equivalent to:

            self.retrieve("product_name == @product_name", {"product_name": product_name}, target_path, use_symlinks)

        NB. A product name is not guaranteed to be unique (only the combination of product type and product name is),
        so this function may retrieve one or more products.

        An exception will be raised if no products with the specified name can be found.

        """
        count = self.retrieve("product_name == @product_name", {"product_name": product_name}, target_path,
                              use_symlinks)
        if count == 0:
            raise Error("no products found with name '%s'" % product_name)
        return count

    def retrieve_by_uuid(self, uuid, target_path=os.path.curdir, use_symlinks=False):
        """Retrieve a product from the archive by uuid.

        This is a convenience function that is equivalent to:

            self.retrieve("uuid == @uuid", {"uuid": uuid}, target_path, use_symlinks)

        An exception will be raised if no product with the specified uuid can be found.

        """
        count = self.retrieve("uuid == @uuid", {"uuid": uuid}, target_path, use_symlinks)
        if count == 0:
            raise Error("product with uuid '%s' not found" % uuid)
        return count

    def retrieve_properties(self, uuid, namespaces=[], property_names=[]):
        """Retrieve product properties for the product with the specified UUID.

        Keyword arguments:
        namespaces  --  List of namespaces of which the properties should be retrieved. By default, only properties
                        defined in the "core" namespace will be retrieved.

        """
        products = self.search(where="uuid == @uuid", parameters={"uuid": uuid}, namespaces=namespaces,
                               property_names=property_names)
        assert len(products) <= 1

        if len(products) == 0:
            raise Error("product with uuid '%s' not found" % uuid)

        return products[0]

    def root(self):
        """Return the archive root path."""
        return self._root

    def search(self, where="", order_by=[], limit=None, parameters={}, namespaces=[], property_names=[]):
        """Search the product catalogue for products matching the specified search expression.

        Keyword arguments:
        where       --  Search expression.
        order_by    --  A list of property names that determines the ordering of the results. If the list is empty, the
                        order of the results in undetermined and can very between calls to this function. Each property
                        name in this list can be provided with a '+' or '-' prefix, or without a prefix. A '+' prefix,
                        or no predix denotes ascending sort order, a '-' prefix denotes decending sort order.
        limit       --  Limit the maximum number of results to the specified number.
        parameters  --  Parameters referenced in the search expression (if any).
        namespaces  --  List of namespaces of which the properties should be retrieved. By default, only properties
                        defined in the "core" namespace will be retrieved.
        property_names
                    --  List of property names that should be returned. By default all properties of the "core"
                        namespace and those of the namespaces in the namespaces argument are included.
                        If this parameter is a non-empty list then only the referenced properties will be returned.
                        Properties are specified as '<namespace>.<identifier>'
                        (the namespace can be omitted for the 'core' namespace).
                        If the property_names parameter is provided then the namespaces parameter is ignored.
        """
        return self._backend.search(where, order_by, limit, parameters, namespaces, property_names)

    def source_products(self, uuid):
        """Return the UUIDs of the products that are linked to the given product as source products."""
        return self._backend.source_products(uuid)

    def strip(self, where="", parameters={}, force=False):
        """Remove one or more products from disk only (not from the product catalogue). Return the number of products
        stripped.

        NB. Depending on product type specific cascade rules, stripping a product can result in one or more derived
        products being stripped (or removed) along with it.

        Keyword arguments:
        where       --  Search expression that determines which products to stip.
        parameters  --  Parameters referenced in the search expression (if any).
        force       --  If set to True, also strip partially ingested products. This affects products for which a
                        failure occured during ingestion, as well as products in the process of being ingested. Use
                        this option with care.
        """
        query = "is_defined(archive_path)"
        if where:
            query += " and (" + where + ")"
        products = self.search(where=query, parameters=parameters,
                               property_names=['uuid', 'active', 'product_name', 'archive_path', 'physical_name'])
        for product in products:
            if not product.core.active and not force:
                raise Error("product '%s' (%s) not available" % (product.core.product_name, product.core.uuid))

            self._strip(product)

        # Strip (or remove) derived products if necessary.
        if len(products) > 0:
            self._establish_invariants()

        return len(products)

    def strip_by_name(self, product_name):
        """Remove one or more products from disk only (not from the product catalogue).

        This is a convenience function that is equivalent to:

            self.strip("product_name == @product_name", {"product_name": product_name})

        NB. A product name is not guaranteed to be unique (only the combination of product type and product name is),
        so this function may strip one or more products.

        An exception will be raised if no products with the specified name can be found.

        """
        count = self.strip("product_name == @product_name", {"product_name": product_name})
        if count == 0:
            raise Error("no products found with name '%s'" % product_name)
        return count

    def strip_by_uuid(self, uuid):
        """Remove a product from disk only (not from the product catalogue).

        This is a convenience function that is equivalent to:

            self.strip("uuid == @uuid", {"uuid": uuid})

        An exception will be raised if no product with the specified uuid can be found.

        """
        count = self.strip("uuid == @uuid", {"uuid": uuid})
        if count == 0:
            raise Error("product with uuid '%s' not found" % uuid)
        return count

    def summary(self, where="", parameters=None, aggregates=None, group_by=None, group_by_tag=False, order_by=None):
        """Return a summary of the products matching the specified search expression.

        Keyword arguments:
        where         --  Search expression.
        parameters    --  Parameters referenced in the search expression (if any).
        aggregates    --  A list of property aggregates defined as "<property_name>.<reduce_fn>".
                          Properties need to be of type long, integer, real, text or timestamp.
                          The reduce function can be 'min', 'max', 'sum', or 'avg'.
                          'sum' and 'avg' are not possible for text and timestamp properties.
                          A special property 'validity_duration' (defined as validity_stop - validity_start) can also
                          be used.
        group_by      --  A list of property names whose values are used for grouping the aggregation results.
                          There will be a separate result row for each combination of group_by property values.
                          Properties need to be of type long, integer, boolean, text or timestamp.
                          Timestamps require a binning subscript which can be 'year', 'month', 'yearmonth', or 'date'
                          (e.g. 'validity_start.yearmonth').
        group_by_tag  --  If set to True, results will also be grouped by available tag values.
                          Note that products will be counted multiple times if they have multiple tags.
        order_by      --  A list of result column names that determines the ordering of the results. If the list is
                          empty, the order of the results is ordered by the `group_by` specification. Each name in the
                          list can have a '+' (ascending) or '-' (descending) prefix, or no prefix (ascending).

        Note that the property names must always include the namespace. 'core' is not assumed.
        """
        return self._backend.summary(where, parameters, aggregates, group_by, group_by_tag, order_by)

    def tag(self, uuid, tags):
        """Set one or more tags on a product."""
        if isinstance(tags, basestring):
            tags = [tags]

        self._backend.tag(uuid, tags)

    def tags(self, uuid):
        """Return the tags of a product."""
        return self._backend.tags(uuid)

    def unlink(self, uuid_, source_uuids=None):
        """Remove the link between a product and one or more of its source products."""
        if isinstance(source_uuids, uuid.UUID):
            source_uuids = [source_uuids]

        self._backend.unlink(uuid_, source_uuids)

    def untag(self, uuid, tags=None):
        """Remove one or more tags from a product."""
        if isinstance(tags, basestring):
            tags = [tags]

        self._backend.untag(uuid, tags)

    def update_properties(self, properties, uuid=None, create_namespaces=False):
        """Update product properties in the product catalogue. The UUID of the product to update will be taken from the
        "core.uuid" property if it is present in the specified properties. Otherwise, the UUID should be provided
        separately.

        This function allows any property to be changed with the exception of the product UUID, and therefore needs to
        be used with care. The recommended way to update product properties is to first retrieve them using either
        retrieve_properties() or search(), change the properties, and then use this function to update the product
        catalogue.

        Keyword arguments:
        uuid    --  UUID of the product to update. By default, the UUID will be taken from the "core.uuid" property.
        create_namespaces  --  Tests if all namespaces are already defined for the product, and creates them if needed

        """
        if create_namespaces:
            if 'core' in properties and 'uuid' in properties.core:
                uuid = properties.core.uuid if uuid is None else uuid
                if uuid != properties.core.uuid:
                    raise Error("specified uuid does not match uuid included in the specified product properties")
            existing_product = self.search(where='uuid == @uuid', parameters={'uuid': uuid},
                                           namespaces=self.namespaces())[0]
            new_namespaces = list(set(vars(properties).keys()) - set(vars(existing_product).keys()))
        else:
            new_namespaces = None
        self._update_metadata_date(properties)
        self._backend.update_product_properties(properties, uuid=uuid, new_namespaces=new_namespaces)

    def verify_hash(self, where="", parameters={}):
        """Verify the hash for one or more products in the archive.
        Returns a list of UUIDs of products for which the verification failed.
        This will be an empty list '[]' if all products match their hash.

        Products that are not active or are not in the archive will be skipped.
        If there is no hash available in the metadata for a product then an error will be raised.

        Keyword arguments:
        where           --  Search expression that determines which products to retrieve.
        parameters      --  Parameters referenced in the search expression (if any).

        """
        failed_products = []
        products = self.search(where=where, parameters=parameters,
                               property_names=['uuid', 'active', 'product_name', 'archive_path', 'physical_name',
                                               'hash', 'product_type'])
        for product in products:
            if 'archive_path' in product.core:
                if 'hash' not in product.core:
                    raise Error("no hash available for product '%s' (%s)" %
                                (product.core.product_name, product.core.uuid))
                if self._calculate_hash(product) != product.core.hash:
                    failed_products.append(product.core.uuid)
        return failed_products
