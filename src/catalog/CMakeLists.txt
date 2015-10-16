add_library(catalog Catalog.cpp CatalogDatabase.cpp CatalogRelation.cpp)
add_dependencies(catalog storage_proto)
