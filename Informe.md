# <span style="color:#3d24c9"> Arquitectura TP2 </span>
En este informe se detalla la arquitectura del sistema distribuido que se encargará de hacer las consultas para el sistema de recomendación de libros de Amazon, todo mientras es tolerante a fallas de los nodos. Tenemos como fin que se trate de un sistema escalable por lo que se ha utilizado una arquitectura de pipe and filter, con lo que cada nodo del sistema anidará múltiples workers que realizarán en paralelo la tarea de un filtro.  
También hemos decidido que en el sistema las consultas se harán _X (secuencial/paralelo)_ por las ventajas y desventajas plasmadas en la siguiente tabla:

## <span style="color:#6e49ad"> Integrantes </span>
- <span style="color:#09ff05"> Diego Civini </span>
- <span style="color:#09ff05"> Facundo Aguirre Argerich </span>

## <span style="color:#9669f0"> Sistema completo </span>
En la siguiente imagen se puede ver como sera la estructura del sistema completo.  
<p align="center"><img src="./images/SistemaCompletoTP2.png" /> </p>

En el TP1 teniamos una estructura en donde varios workers de un mismo stage leian de una misma cola y luego escribian en una misma cola. De esta manera da uno iba agarrando mensajes a medida que estuviese disponible. Pero para este TP, se pedia implementar multiples clientes paralelos. Con nuestra vieja estructura esto se nos hacia muy dificil de coordinar y nos traia mas problemas que comodidades. Es por eso que se decidio cambiar toda la estuctura. Para este nuevo TP, cada worker tiene su cola de salida como de entrada. No sucede mas que varios workers leen en una misma cola o escriben en una misma cola. De esta manera, la coordinacion de los EOFs y demas mensajes se hace mucho mas simple, ya que no existe el problema de que un worker le robe un EOF a otro worker. Procedemos a mostrar mas de cerca como se veria por ejemplo la Q1 con esta nueva estructura.
<p align="center"><img src="./images/PipelineQ1.png" /> </p>
En la Q1 ahora tenemos 9 colas (y esto porq hay 9 workers porque si hubiese mas, habria mas colas) donde antes habria solo 3 colas (sin importar la cantidad de workers). 

Con esta nueva estructura, hay que tener una manera de saber a que worker mandar el mensaje y como manejar los EOFs. Para entender esto, tenemos los siguiente ejemplos:

## <span style="color:#9669f0"> Diagrama de robustez </span>
A continuación podemos observar el diagrama de robustez que nos indica cómo se relacionan las entidades del sistema y la manera de comunicación entre ellas mediante boundaries, controllers y entities.  
![](./images/DiagramaRobustez.png)

## <span style="color:#9669f0"> Diagrama de despliegue </span>
En el diagrama de despligue podemos ver como se agrupan los diferentes nodos del sistema en diferentes grupos y como se comunican entre ellos.
<p align="center"><img src="./images/DiagramaDespliegue.png" /> </p>


## <span style="color:#9669f0"> Diagrama de logica de Fallas </span>
En nuestro sistema tenemos varios tipos de workers, y cada tipo es distinto a la hora de manejar las fallas. La mayoria podemos encasillarlos en 2 tipos distintos. Por un lado tenemos la abstraccion Worker. En esta se encuentran la mayoria de los workers del sistema, y podemos dividirla en 2 sub-abstracciones. Por un lado el NoStateWorker, en el cual podemos encontrar workers que no tienen estado de resultados ya que lo unico que hacen es filtar o modificar datos (por ejemplo los filtros de titulos, categorias, etc). La otra sub-abstraccion es el StateWorker que en este caso si tiene un estado por cada cliente en el que va acumulando resultados (por ejemplo el worker que genera el percentil o el que consigue los tops).
El otro tipo importante es el MultipleQueueWorker. En la abstraccion que hablamos antes, los datos son recibidos por una sola cola. En el MultipleQueueWorker los datos son recibidos por mas de una cola. Esto provoca que se tengan que tener mas consideraciones a la hora sincronizar datos ya que un cliente no termina hasta que se terminen los datos de todas las colas sobre ese cliente. Este tipo de worker lo podemos ver en los JoinWorkers y el ResultsCoordinator. Por ejemplo, en el JoinWorker se reciben datos de 2 colas, una de titulos y otra de reseñas. Entonces para un mismo cliente, el sistema debe aguardar que llegen todos los datos de ambas colas para poder decir que termino con ese cliente.
<p align="center"><img src="./images/WorkersClases.png" /> </p>

### <span style="color:#9669f0"> StateWorker </span>
Empezamos con uno de los mas simples. El NoStateWorker. Por cada mensaje que recibe, primero debe fijarse si se trata de un mensaje de EOF o un mensaje de datos. Si es un mensaje de datos, debe fijarse el id del mismo (msg_id) y del cliente. Si para ese cliente ya se habia recibido un mensaje con ese msg_id, entonces se esta recibiendo un mensaje repetido. En este caso directamente se le hace ack inmediatamente ya que no nos sirve para nada mas. En caso contrario, hay que guardarse ese msg_id. A partir de ahi, si el mensaje es de un cliente nuevo, este tendra un client_id que no se tenia registrado en _active_clients_. Una vez registrado se escribe en memoria el nuevo _active_clients_ y se procede a procesar, enviar y ackear el mensaje. 
Por el lado del EOF, para chequear repetidos, cada mensaje EOF contiene el id del cliente y el id del worker de donde viene. De esta manera si me llega un EOF del cliente con id 4 desde el worker con id 0, ya se que para ese mismo cliente no me puede llegar un EOF desde ese mismo worker. Cada delivery_tag de los EOF es guardado hasta que lleguen todos los EOFs necesarios (el manejo de los EOFs esta explicado al principio en la seccion **Sistema Completo**). Una vez que sucede, se mandan los EOFs a la siguiente etapa, se actualiza en disco el _active_clients_ y se hace ACK de todos los EOFs que se habian acumulado.

<p align="center"><img src="./images/DiagramaFallasNoStateWorker.png" /> </p>


## <span style="color:#09ff05">**Protocolo de comunicacion y serialiazacion**</span>
En cuanto a la serializacion, sigue siendo igual de como se explico en el TP1. Pero se hicieron un par de agregados. Primero, se necesitaba una forma de identificar los mensajes de los distintos clientes que estan en paralelo. De esto se encarga el Server. Por cada conexion que recibe de un cliente nuevo, el Server le asigna un id que es un valor incremental. Desde ahi, cada mensaje recibido de ese cliente tiene delante el id del cliente. De esta manera, cada worker sabe que cuando recibe un mensaje debe separar lo que es el contenido del mensaje con el id del cliente.
Ademas de este id del cliente, cada mensaje tiene un id propio. Este id nace desde la libreria que tiene el cliente, la cual le agrega un id al mensaje antes de mandarlo para que despues cuando el server haga ACK, este sepa de que mensaje se esta hablando. Este id luego se usa en el sistema para que los workers los usen para identificar mensajes repetidos en caso de que los haya.
Entonces, cada worker cuando le llega un mensaje sabe que tiene que sacarle el id del mismo y el del cliente. Todo esto se hace con una misma funcion del modulo de serializacion.
