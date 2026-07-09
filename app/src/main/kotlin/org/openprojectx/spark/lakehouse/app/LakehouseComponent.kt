package org.openprojectx.spark.lakehouse.app

import dagger.Component
import javax.inject.Singleton
import org.openprojectx.spark.boot.dagger.BuiltinConnectorModule
import org.openprojectx.spark.boot.dagger.RuntimeModule
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.boot.dagger.SparkModule
import org.openprojectx.spark.lakehouse.catalog.CatalogConnectorModule
import org.openprojectx.spark.lakehouse.gold.GoldConnectorModule
import org.openprojectx.spark.lakehouse.ingestion.LakehouseConnectorModule
import org.openprojectx.spark.lakehouse.silver.SilverConnectorModule

/**
 * spark-boot component extended with the lakehouse node factories. This is
 * the single runtime wiring for every job template in the image.
 */
@Singleton
@Component(
    modules = [
        SparkModule::class,
        RuntimeModule::class,
        BuiltinConnectorModule::class,
        CatalogConnectorModule::class,
        LakehouseConnectorModule::class,
        SilverConnectorModule::class,
        GoldConnectorModule::class,
    ]
)
interface LakehouseComponent : SparkBootComponent
