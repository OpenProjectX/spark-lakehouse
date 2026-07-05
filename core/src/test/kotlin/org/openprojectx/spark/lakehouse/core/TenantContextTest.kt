package org.openprojectx.spark.lakehouse.core

import com.typesafe.config.ConfigFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class TenantContextTest {

    @Test
    fun `parses tenant block and derives layout`() {
        val config = ConfigFactory.parseString(
            """
            tenant {
              id = "acme"
              storage-root = "s3a://lake/acme/"
              tags { team = "sales" }
            }
            """.trimIndent()
        )

        val tenant = TenantContext.from(config)

        assertEquals("acme", tenant.tenantId)
        assertEquals("s3a://lake/acme/bronze/orders", tenant.layerPath(Layer.BRONZE, "orders"))
        assertEquals("acme_silver", tenant.namespace(Layer.SILVER))
        assertEquals(mapOf("team" to "sales"), tenant.tags)
    }

    @Test
    fun `rejects missing tenant block with actionable message`() {
        val error = assertThrows<JobConfigException> {
            TenantContext.from(ConfigFactory.empty())
        }
        assertTrue(error.message!!.contains("tenant"))
    }

    @Test
    fun `rejects invalid tenant id`() {
        val config = ConfigFactory.parseString(
            """
            tenant { id = "Acme Corp", storage-root = "file:///lake/acme" }
            """.trimIndent()
        )
        val error = assertThrows<JobConfigException> { TenantContext.from(config) }
        assertTrue(error.message!!.contains("tenant.id"))
    }
}
