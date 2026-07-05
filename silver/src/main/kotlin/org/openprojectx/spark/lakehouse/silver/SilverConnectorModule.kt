package org.openprojectx.spark.lakehouse.silver

import dagger.Binds
import dagger.Module
import dagger.multibindings.IntoMap
import dagger.multibindings.StringKey
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.UntypedNodeFactory
import org.openprojectx.spark.lakehouse.core.LakehouseNodeKinds

/** Contributes silver-layer node factories to the spark-boot registries. */
@Module
interface SilverConnectorModule {

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.CDC_RESOLVE_TRANSFORM)
    fun bindCdcResolveFactory(factory: CdcResolveTransformNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.CDC_RESOLVE_TRANSFORM)
    fun bindCdcResolveConfigFactory(factory: CdcResolveTransformConfigFactory): ConfigNodeFactory

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.ICEBERG_MERGE_SINK)
    fun bindIcebergMergeFactory(factory: IcebergMergeSinkNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.ICEBERG_MERGE_SINK)
    fun bindIcebergMergeConfigFactory(factory: IcebergMergeSinkConfigFactory): ConfigNodeFactory
}
