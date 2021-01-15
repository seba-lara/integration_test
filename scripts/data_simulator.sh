#!/bin/bash
clear

function help {
  echo ""
  echo "  -a :      Envia mensajes por AMQP"
  echo "  -m :      Envia mensjaes por MQTT"
  echo "  -h :      Muestra este menú"
  echo ""
  echo "paso 1.     Ingrese la ip del host"
  echo "paso 2.     Ingrese el nombre del archivo de datos.csv "
  echo ""
}

echo "####################################"
echo "###### PRUEBA DE INTEGRACION! ######"
echo "####################################"
########################################################
#DESPLEGAR AYUDA
if [[ $1 == -h ]]; then
  help
else
#COPIAR LLAVES SSH
  VAR=$(ls -la $HOME/.ssh | grep 'si_key')
  if [[ $VAR != 1 ]]; then
    cp ../keys/si_key $HOME/.ssh/si_key
    chmod 600 $HOME/.ssh/si_key
    cat ../keys/si_key.pub >> $HOME/.ssh/authorized_keys
  else
    echo "Las llaves ssh se encuentran activas!"
  fi
#INGRESAR DATOS
  if [[ $1 == -m ]] || [[ $1 == -a ]]; then
    sleep 2
	  echo "Ingrese la ip del host de destino : "
	  read HOST_DESTINO
	  echo "Ingrese el nombre del archivo de datos.csv : "
	  read DATOS
    sed -i "s/host:.*/host: $HOST_DESTINO/g" ../src/csv-enqueuer/csv-enqueuer.conf
    TEST=$(curl ${HOST_DESTINO}:27017)
	  REST=('It looks like you are trying to access MongoDB over HTTP on the native driver port.')
#ELIMINAR COLECCIONES EN LA BD
    if [[ $REST == $TEST ]]; then
			ssh -i $HOME/.ssh/si_key -o "StrictHostKeyChecking=no" root@${HOST_DESTINO} "docker exec -i si_mongo_1 mongo polin -u si -p tisapolines --eval 'db.measurements.remove({})' --quiet"
			ssh -i $HOME/.ssh/si_key -o "StrictHostKeyChecking=no" root@${HOST_DESTINO} "docker exec -i si_mongo_1 mongo polin -u si -p tisapolines --eval 'db.statuses.remove({})' --quiet"
			ssh -i $HOME/.ssh/si_key -o "StrictHostKeyChecking=no" root@${HOST_DESTINO} "docker exec -i si_mongo_1 mongo polin -u si -p tisapolines --eval 'db.notifications.remove({})' --quiet"
			ssh -i $HOME/.ssh/si_key -o "StrictHostKeyChecking=no" root@${HOST_DESTINO} "docker restart si_mongo_1 "
      ssh -i $HOME/.ssh/si_key -o "StrictHostKeyChecking=no" root@${HOST_DESTINO} "docker restart si_web_1 "
      echo "Espere un momento mientras se reinicia el modulo web..."
      sleep 60
#ENVIO DE DATOS SEGUN SU PROTOCOLO
			if [[ $1 == -a ]]; then
				echo "Se enviaran datos mediante AMQP"
        sleep 2
        python2 $PWD/../src/csv-enqueuer/csv-enqueuer.py -c $PWD/../src/csv-enqueuer/csv-enqueuer.conf -f $DATOS -a
      else [[ $1 == -m ]]
				echo "Se enviaran datos mediante MQTT"
        sleep 2
				python2 $PWD/../src/csv-enqueuer/csv-enqueuer.py -c $PWD/../src/csv-enqueuer/csv-enqueuer.conf -f $DATOS
			fi
    else
		echo "Error al tratar de conectar con la BD, intentelo nuevamente"
		fi
  else
	echo "Error en alguno de los parametros ingresados. Utilice el argumento -h para desplegar la ayuda"
  fi
fi
