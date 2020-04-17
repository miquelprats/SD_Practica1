This project is a distributed implementation of the multiplication of two matrices.
One matrix is based on the m and n paramaters, and the other one is based on n and l.
Both matrices can be modified at the start of the 'MultiplicacioMatriu.py' file with their respective values.

About the distributed implementation, it's row-based for A matrix and column-based for B matrix, the parameter that defines the value of that is 'a', which can also be modified at the 'MultiplicacioMatriu.py' file.

This version is using the IBM cloud object storage bucket.
For using the object storage, 'the cos_backend.py' controls the connection.
You need to use your own credentials, for that you need to modify the cos_backendAUX.py, adding your 'service_endpoint', 'secret_key', 'acces_key'. 
After that there's the final step of modifying the import, from cos_backend import COSBackend --> from cos_backendAUX import COSBackend in 'MultiplicacioMatriu.py'.