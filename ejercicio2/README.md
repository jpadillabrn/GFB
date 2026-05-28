# Ejercicio 2  
----

Considera el siguiente escenario asumiendo el rol de ingeniero de datos:  
El área operativa del departamento de TI tiene tres aplicaciones. Para cada 
aplicación su fuente de datos es de tipo transaccional y las tres funcionan 24/7. 
Llamemosles F1, F2 y F3 respectivamente, las cuales poseen las siguientes 
características:  
● F1 es un CRM propietario. Contiene información sobre los clientes e.g. 
demográficos y datos de contacto.  
● F2 tiene un RDBMS SQL Server. Almacena las transacciones de los clientes 
sobre la mitad de los productos que ofrece la compañía.  
● F3 tiene un RDBMS Postgresql. Almacena las transacciones de los 
clientes sobre el resto de los productos.  
Ten en cuenta los siguientes supuestos:  
● Las tres fuentes de datos F1, F2 y F3 fueron diseñadas de manera 
independiente tanto a nivel de software como de modelo de datos.  
● Como las fuentes de datos son operativas diariamente hay datos nuevos. 
 
  
Tu objetivo es diseñar una arquitectura que consolide la información contenida en 
las fuentes de datos de F1, F2 y F3 que cumpla con dos propósitos: en primer lugar 
que habilite a un grupo de usuarios del área operativa a la extracción de consultas  
por medio de SQL y de manera secundaria que permita al equipo de ciencia de 
datos la aplicación de algoritmos de detección de patrones como clustering o 
búsquedas en grafos.  
Tu diseño puede utilizar herramientas de almacenamiento y procesamiento 
diferentes para cada propósito si lo consideras necesario. 
   
 
Instrucciones (ejercicio 2):  
 
No olvides que las respuestas a las preguntas deben de argumentarse en el 
entregable:  
I. Propón una arquitectura (solo proponla no la construyas) que considere 
los siguientes aspectos y preguntas:  
A. De cada fuente de datos se tienen identificados que campos requiere 
el área operativa. ¿Para cumplir con los dos objetivos que  
subconjunto de cada fuente de datos extraerías?  
B. ¿Qué posibles retos implica la extracción de cada una de las  
fuentes de datos por separado y qué herramientas utilizas ?  
C. ¿Qué posibles retos implica la independencia en el modelo de 
datos de las tres fuentes y cómo los resolverías?  
D. ¿Aparte de un proceso batch en la hora de menor uso, cómo  
podrías mitigar el impacto de tu pipeline sobre las fuentes originales ? 
E. ¿Cuáles etapas considerarías en tu proceso de transformación de datos 
y qué uso les darías?  
F. ¿Qué herramientas utilizas para las etapas de transformación? 
G. ¿Qué storage usarías para cada propósito y por qué ?  
H. Recuerda que al menos a diario tendrás que llevar data nueva a tu 
etapa de transformación final, ¿Como orquestarias tu pipeline y con 
qué herramienta?  
I. Proporciona un diagrama de tu propuesta de arquitectura.  
II. Seguridad (manteniendo tu rol de ingeniero de datos).  
A. ¿Cómo mantendrías la seguridad de tu flujo de datos end-to-end? Es 
decir disminuir riesgos de posibles fugas o intrusiones no deseadas al 
entorno de ejecución que estás construyendo.  
III. Gobernanza de datos  
A. ¿Cómo llevarías control de la metadata y sus cambios al igual que los 
procesos de tu pipeline y cómo almacenamos estos datos? 