package mcpserver

func SchemaZeroForTest[Out any]() Out { return schemaZero[Out]() }
