# Informe — TP Coordinación


## 1. Identidad de consulta

Cada cliente que se conecta recibe un identificador de consulta que el Gateway estampa, a
través de su `MessageHandler`, en todo lo que manda hacia el sistema. Es el único punto donde
existe la noción de "un cliente nuevo", y el protocolo interno lleva ese id en cada
mensaje.

Los controles no tienen un pipeline por cliente sino que  tienen una partición de estado por consulta sobre las mismas colas y los mismos procesos. Cada partición se crea con el primer
dato de esa consulta y se destruye al emitir su resultado, así que una consulta resuelta no
deja nada acumulado.

El camino de vuelta usa el mismo identificador, el Gateway le ofrece cada resultado a los clientes conectados y lo entrega al que lo reconoce como propio.

---

## 3. Coordinación entre las instancias de Sum

Los registros de un cliente se reparten entre las N réplicas de Sum a través de una *work
queue*, con un mensaje sin confirmar por consumidor. Ese límite es el que hace que el reparto
sea proporcional a la velocidad real de cada réplica, con un límite mayor, el broker adelanta batches y una réplica rápida puede acaparar trabajo mientras otra queda libre sin hacer nada.

Como consecuencia del reparto, la notificación de fin de ingesta la recibe 1 sola réplica.
Esa réplica la difunde a las demás por un exchange de control propio del grupo de Sums, y
entonces las N vuelcan sus totales parciales hacia las instancias de Aggregation.

### 3.1. Las race conditions y cómo se resuelve

Vemos que Difundir no alcanza, ya que cuando una réplica se entera de que la consulta terminó, otras pueden tener registros de ese mismo cliente todavía en proceso, el broker se los entregó, pero todavía no los procesaron. Si volcaran en ese momento, esos registros se perderían y el total
saldría por debajo del que realmente es, de forma intermitente.

La solución es agregar una restricción de diseño, cada Sum consume la
cola de datos y su cola de control por el mismo canal de la misma conexión, con un único hilo de despacho. Partiendo de 2 premisas:

1. La notificación de fin es el último mensaje de esa consulta en la cola, y el broker
   asigna los mensajes en orden de cola. Por lo tanto, cuando se entrega la notificación, todos
   los registros anteriores de esa consulta ya fueron entregados a alguna réplica.
2. Lo que se entregó a una réplica y la notificación que se publica después viajan por **la
   misma conexión**, y el cliente AMQP los procesa en el orden en que llegan.

De las dos se ve que, al despacharse la notificación, no queda ningún registro de esa
consulta sin procesar. La cantidad de mensajes pendientes es cero, y no depende del límite de
mensajes sin confirmar que se haya configurado.

---

## 4. Coordinación entre Sum y Aggregation

Cada Sum no le manda sus totales a todas las instancias de Aggregation, sino que particiona las frutas por clave, con una función determinística sobre el nombre de la fruta módulo la cantidad de
réplicas, y le manda a cada una únicamente las frutas que tiene a cargo. Es la función de
partición del modelo MapReduce, donde los Sums serían los map workers y los Aggregation serían los reduce workers.

El determinismo se plantea como un requisito, haciendo que todas las réplicas de Sum
deban mandar la misma fruta a la misma instancia de Aggregation. 

Cada instancia de Aggregation consolida esperando las notificaciones de fin de ingesta de
todas las réplicas de Sum antes de calcular su top parcial. Las réplicas de Sum a las que no
les tocó ninguna fruta para esa instancia mandan igual su notificación, porque de ella depende
que la consulta se cierre.

---

## 5. Consolidación final en el Join

El Join espera un top parcial de cada instancia de Aggregation (incluso vacío, si a esa instancia no le tocó ninguna fruta de la consulta) y recién entonces calcula el top final.

Que el merge sea correcto depende de que la partición sea por clave. Si una fruta está en el
top K global, hay a lo sumo K−1 frutas que la superan en todo el sistema; las que la superan
dentro de su propia partición son un subconjunto de esas, o sea a lo sumo K−1 también. Por lo
tanto está en el top K de su partición, y el top K global está contenido en la unión de los top K parciales. Si una fruta pudiera estar en dos instancias de Aggregation, ni el merge ni
la suma de totales serían correctos.

El mismo mecanismo de consolidación por conteo se aplica entonces en dos lugares del sistema: entre Sum y Aggregation, y entre Aggregation y Join.


---

## 6. Escalabilidad

### Respecto a los clientes

El sistema no replica infraestructura por cliente, sino que multiplexa todas las consultas sobre las mismas colas, exchanges y procesos, y las separa por el id de consulta. Lo que crece con la cantidad de consultas concurrentes es la memoria de los controles (acotada por la cantidad de frutas distintas de cada consulta, no por la cantidad de registros que mandó el cliente) y no la cantidad de colas ni de procesos. El estado de cada consulta se libera al resolverla.

### Respecto a grandes volúmenes de datos

Los registros de cada cliente se reparten entre las N réplicas de Sum, así que la ingesta
escala agregando réplicas. La cantidad de información que viaja hacia adentro del sistema se
mantiene acotada en tres puntos:

- El Sum no emite un mensaje por registro sino uno por consulta y por destino, con sus
  frutas ya totalizadas. El tamaño de ese mensaje depende de la cantidad de frutas distintas, no del volumen de registros recibidos.
- La partición por clave hace que cada fruta viaje a 1 solo destino en vez de a todos, lo que reduce el tráfico hacia las instancias de Aggregation en proporción a su cantidad.
- Aggregation y Join no propagan datos completos hacia adelante sino tops acotados por el tamaño pedido.

### Respecto a la cantidad de controles

Agregar réplicas de Sum reparte la ingesta, mientras que agregar réplicas de Aggregation reparte la
totalización y la memoria, porque cada una es responsable de aproximadamente una fracción de las claves.

En cuanto al protocolo de control, ningún control le habla a todos los demás en todas las
etapas, ya que la difusión del fin de ingesta ocurre 1 vez por consulta entre las réplicas de Sum, y las notificaciones hacia Aggregation acompañan al volcado de datos que en cualquier caso hay que hacer. No hay ningún intercambio que crezca con la cantidad de registros procesados.

---

## 7. Secciones críticas y liberación de recursos

Los tres controles son de 1 solo hilo, un único ciclo de consumo que atiende
secuencialmente todas sus fuentes. No hay estado compartido entre hilos, así que no hay región
crítica que proteger. Esto nace por la decisión de la sección
3.1, donde la alternativa de consumir datos y control en hilos separados habría requerido un mutex sobre los acumuladores y, aun con eso, no habría cerrado la condición de carrera, porque el mensaje pendiente no está siendo procesado sino esperando a ser despachado. El diseño de un solo hilo elimina la necesidad de sincronizar.

Los tres controles manejan SIGTERM. La señal no puede simplemente encender un flag, porque
el proceso está bloqueado en una llamada que, al ser interrumpida, se reintenta; hace falta un
mecanismo que despierte el ciclo de eventos. El handler encola el pedido de corte en el
ciclo, que lo ejecuta entre mensajes, por lo que el que se esté procesando termina y se confirma antes
de cortar, y recién entonces se cierran el canal y la conexión y el proceso termina
normalmente.

En el caso de Sum, que es el control con más bocas, todas ellas (la cola de datos, las dos de
control y una por cada instancia de Aggregation) comparten 1 sola conexión contra el
middleware. Además de ser lo que hace correcta la sincronización descrita en la sección 3.1,
eso mantiene acotada la cantidad de conexiones contra el broker cuando se escala la cantidad de
réplicas.
