El informe va a ser largo principalmente por la race condition en los Sums con el EOF. Lo separo por secciones para que sea mas facil de entender que hice.

Identificacion de cliente

Al generar un ID por cliente en el gateway (especificamente en el handler, con uuid), puede enviarse a las distintas entidades internas del sistema mediante el protocolo interno, de forma que tanto sum, agregation y join puedan realizar sus operaciones distinguiendo el cliente.
Con esto, es posible deserializar el mensaje enviado por join con el top de frutas pero agregando en el mensaje tambien el id de cliente. Como el deserealizar esta en message handler, puede comparar si el id del cliente recibido es el correspondiente al handler, y si no lo es filtra el top (no es el que le corresponde), ya que el join envia a la queue un mensaje que llega al gateway, y este se lo da a cada handler para que lo trabaje como corresponda. Ese mensaje, ahora con el id de cliente, es lo que permite descartar el top si es el que corresponde a otro cliente.
Ahora mismo, la información se guarda como se hacia originalmente en sum y agregation, pero se agrego un nivel mas en los diccionarios con el client_id para separar la data de cada cliente (diccionarios en init). Con esto, es posible recibir mensajes de N clientes y procesarlos en el mismo sum/agregation sin que se mezcle la información de cada uno. Ante muchos clientes, la cantidad de información aumenta considerablemente. Por eso, ante un EOF, reutilice el delete de la catedra en el aggregation pero para borrar la data correspondiente al cliente que termino.

Coordinacion de los sum

Agregue un exchange en el init de cada sum, de forma que escuche mensajes de los otros N-1 SUMs y que tambien pueda enviarles mensajes (hay varias conexiones por nodo SUM, una sola escucha). De esta forma, pueden comunicarse los SUM_AMOUNT entre si. Esto lo hago en un hilo aparte del start_consuming, de forma de procesar mensajes del gateway en un hilo distinto al de coordinacion entre sums.
Con esto, al recibir un mensaje EOF de un cliente en particular, el sum que lo recibe pasa a considerarse el sum coordinador de ese cliente, y envia un mensaje al resto de los sums por el hilo nuevo indicando que llego el ultimo mensaje de ese cliente. Al recibirlo, todos los nodos saben que deben enviar sus resultados de ese cliente a un aggregation.
Esto genera un problema. Puede haber race condition si no se proceso algun mensaje de ese cliente en otro sum cuando llego el EOF del coordinador, por lo que tenía que modificar la solución.

Solución de la Race Condition en cordinación de SUMs

Esta fue la parte mas complicada de solcuionar. Se me ocurrio hacer que el coordinador no mande el EOF al resto de sums, sino que solicite la cantidad de mensajes recibidos de ese cliente por parte de cada uno. La idea es que como el handler que envia mensajes a los sums es 1 por cliente, puedo contar cuantos mensajes envio, y en el EOF agregar un campo numerico que indique esa cantidad, de forma que el coordinador sepa cuantos mensajes circularon entre todos los sums.
Con esto, ante este primer mensaje, cada sum le informa cuantos mensajes de ese cliente llegaron y guarda quien es el sum coordinador de ese cliente (otro diccionario en el init, el ID del coordinador lo envia el mismo para que le sigan enviando mensajes si es necesario). En el caso feliz, la suma de los N counts da la cantidad de mensajes total del cliente y puede ahora si enviarse el EOF y todos envian sin problema la data del cliente al aggregation.
En el caso que no de lo mismo, nadie envia nada. Algun sum procesara mas adelante 1 o mas mensajes faltantes de ese cliente. En ese momento, ese sum le avisa al coordinador que termino de procesar un mensaje mas, y el coordinador, al recibir el mensaje, actualiza el count y compara de nuevo. Esto se repite hasta que el coordinador determina que todos los mensajes fueron procesados, y en ese momento manda el mensaje EOF al resto de sums.

Una cosa que quiero aclarar. Originalmente mi idea no era guardar el coordinador para posterior comunicacion en caso de ser necesaria. La idea era hacer broadcast al resto de sums por el exchange, recibir N-1 respuestas y corroborar la suma. Si fallaba, hacer un timer, volver a hacer el broadcast, y volver a sumar. Esto dentro de un ciclo con la suma de cantidad de mensajes como condicion de corte.
No me gustaba la idea del timer, y mientras lo hacia me di cuenta de que podia enviar mas mensajes por el exchange, de forma de evitar depender de un tiempo especifico y poder coordinar en base a mensajes de comunicacion entre sum. Fue en este punto que me di cuenta que, en escencia, mi idea era hacer una barrera.

Aggregation

Recibe los resultados de N sums, siendo M aggregations. El hash usado hace que a un aggregation lleguen datos dependientes al nombre de la fruta, de forma que un aggregation tiene toda la cantidad de una fruta de un cliente. Esto hace que los tops sean consistentes. Para saber que no van a llegar mas datos, debe esperar N mensajes EOF (misma idea que en sum, la reutilice) para saber que los N sums, sobre ese cliente, ya enviaron todo (aca cada sum si hace un broadcast).
Ante los N EOFs recibidos, los M aggregations pasan al join su top local de ese cliente.

Join

Como son M aggregations, espera hasta recibir M tops parciales de un cliente en particular. Por cada top recibido, va actualizando sus datos. Al recibir el ultimo, envia el top final al gateway.

Escalado

Por todo lo explicado, la solucion sirve para multiples clientes. Cada uno trabaja de forma independiente, y cada entidad interna sabe trabajarlos por separado. En cuanto a los datos, intente mantener la menor cantida de informacion necesaria en los diccionarios en todo momento.
Cada vez que envio un EOF a la siguiente entidad con respecto a un cliente, borro o limpio (dependiendo de necesidad, el limpiar lo digo mas por el caso del -1 en el sum debido a la race condition) los datos correspondientes. Como el sistema es, en escencia, un pipeline de operaciones Map Reduce, donde cada vez tengo menos data, los datos de un cliente van pasando de los N sums a los M agregation, y luego al join.
Como el sum es la primer entidad de este pipeline, es la que mas datos maneja. Ademas, es la que mas controles necesita, pues deben cordinarse entre si para terminar con un cliente y enviar todos sus datos. El aggregation solo necesita recibir N EOFs para saber que debe enviar sus resultados al join, y trabaja de forma independiente al resto, sin necesidad de cordinarse. El join, por su parte, ni siquiera requiere de EOFs. Simplemente recibe los M tops y con eso sabe que llego todo lo que corresponde a ese cliente.

2 aclaraciones mas que quiero dejar en el informe. Los mensajes que hacen broadcast (a mi mismo no, como aclare antes es a los otros N-1) son los EOFs de cordinación de SUMs y el solicitar cantidad de un cliente, tambien envivado por el sum coordinador. Entre entidades, solo el EOF de cliente se envia a cada agregation. Los mensajes con contenido se envian siempre a un destinatario solo.
Por ultimo, la creación de conexiones en el exchange de coordinación de los SUMs no es ideal. Estoy creando una conexión por cada nodo (N*N en total) entre las de enviar y la de escuchar que tiene cada uno.
