package org.openprojectx.spark.lakehouse.catalog

import dagger.Binds
import dagger.Module
import dagger.multibindings.IntoMap
import dagger.multibindings.StringKey
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.UntypedNodeFactory
import org.openprojectx.spark.lakehouse.core.LakehouseNodeKinds

/** Contributes catalog-table node factories to the spark-boot registries. */
@Module
interface CatalogConnectorModule {

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.TABLE_SOURCE)
    fun bindTableSourceFactory(factory: TableSourceNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.TABLE_SOURCE)
    fun bindTableSourceConfigFactory(factory: TableSourceConfigFactory): ConfigNodeFactory

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.ICEBERG_ZERO_COPY_APPEND_ACTION)
    fun bindZeroCopyAppendFactory(factory: IcebergZeroCopyAppendActionNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.ICEBERG_ZERO_COPY_APPEND_ACTION)
    fun bindZeroCopyAppendConfigFactory(factory: IcebergZeroCopyAppendActionConfigFactory): ConfigNodeFactory
}
