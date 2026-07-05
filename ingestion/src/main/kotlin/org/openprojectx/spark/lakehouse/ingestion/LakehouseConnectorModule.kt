package org.openprojectx.spark.lakehouse.ingestion

import dagger.Binds
import dagger.Module
import dagger.multibindings.IntoMap
import dagger.multibindings.StringKey
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.UntypedNodeFactory
import org.openprojectx.spark.lakehouse.core.LakehouseNodeKinds

/**
 * Contributes lakehouse node factories to the spark-boot registries. Include
 * alongside spark-boot's SparkModule/RuntimeModule/BuiltinConnectorModule in
 * the application component.
 */
@Module
interface LakehouseConnectorModule {

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.BRONZE_SNAPSHOT_SINK)
    fun bindBronzeSnapshotSinkFactory(factory: BronzeSnapshotSinkNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.BRONZE_SNAPSHOT_SINK)
    fun bindBronzeSnapshotSinkConfigFactory(factory: BronzeSnapshotSinkConfigFactory): ConfigNodeFactory
}
