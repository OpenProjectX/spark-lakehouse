package org.openprojectx.spark.lakehouse.gold

import dagger.Binds
import dagger.Module
import dagger.multibindings.IntoMap
import dagger.multibindings.StringKey
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.UntypedNodeFactory
import org.openprojectx.spark.lakehouse.core.LakehouseNodeKinds

/** Contributes gold-layer node factories to the spark-boot registries. */
@Module
interface GoldConnectorModule {

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.SCD2_DIM_SINK)
    fun bindScd2DimSinkFactory(factory: Scd2DimSinkNodeFactory): UntypedNodeFactory

    @Binds
    @IntoMap
    @StringKey(LakehouseNodeKinds.SCD2_DIM_SINK)
    fun bindScd2DimSinkConfigFactory(factory: Scd2DimSinkConfigFactory): ConfigNodeFactory
}
