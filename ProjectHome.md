ORM-HBase

This is simple orm for HBase, the project includes several parts:

DataMapper

According to PoEAA, domain object knows nothing about Data Mapper, but mapper knows the domain object. Actually data mapper is designed for specific objet including functions to access databases.

From PoEAA
Here, the ORM-HBase is designed based on the model, but make it more general. We specify the mapping from object’s fields to HBase schema through annotation, and design a general DataMapper based on generic data type (with annotations).

Object Beans:
Make sure the object has an empty construction function with no parameters and getter&setter functions for each member variable;

Dao:
User does not need to call Create() function of DataMapperFactory nor need to know anything about DataMapper’s functions. Only need to use Dao to do CRUD.


---
