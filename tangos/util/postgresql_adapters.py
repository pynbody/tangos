def register_postgresql_adapters():
    from psycopg2.extensions import register_adapter, AsIs
    import numpy as np
    
    asis_adapter = lambda x : AsIs(x)
    register_adapter(np.float64, asis_adapter)
    register_adapter(np.int64, asis_adapter)
    register_adapter(np.float32, asis_adapter)
    register_adapter(np.int32, asis_adapter)
