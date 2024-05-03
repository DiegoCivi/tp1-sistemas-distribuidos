# Trabajo Practico 1: Escalabilidad

## Integrantes
- **Facundo Aguirre Argerich**: 107.539
- **Diego Civini**: 107.662

## Instructivo de uso

Para poder ejecutar el books analyzer, se debe insertar primero manualmente los datasets deseados a probar en la carpeta `datasets/`. Luego, se debe ejecutar el script `run.sh` con el siguiente comando:

```bash
bash run.sh
```

Puede que tengamos que darle permisos suficientes debido a docker si es que no lo tenemos en un grupo de usuarios que tenga permisos de ejecución. Para ello, podemos usar el siguiente comando:

```bash
sudo bash run.sh
```
Una vez ejecutado, deberemos esperar el tiempo correspondiente al procesamiento de los datos proveidos por los datasets. Pasado el tiempo vamos a poder ver como respuesta los integrantes o filas de los datasets que cumplan con las condiciones de las queries solicitadas. Esto se imprimirá en el contenedor del cliente, quien es el que inicialmente provee los datasets a analizar.  
Para poder detener la ejecución del proyecto podemos simplemente usar un `Ctrl+C` por consola, o utilizar el comando `bash stop.sh` para detener el contenedor de forma segura.   
Para más detalles sobre la arquitectura propuesta ver el archivo `Informe.md`.

## Video de ejecución con dataset original
A continuación se adjunta el link al video donde se corre el programa con la totalidad de datos del dataset original y se muestran los resultados obtenidos por consola.  
[Video de ejecución con dataset original](https://drive.google.com/file/d/1vrVZZPmQ2HEF5zaexbdIbN3flbJRuJyP/view?usp=drive_link)

## Known issues (Cosas que sabemos que ahora mismo no funcionan)
- No se handlea signals, por lo que si bien se liberan los recursos al final de la ejecución de cada container, si se induce  una señal _SIGTERM_, ahora mismo no se liberan los recursos. Esto se debe a que no se llegó con el tiempo a implementar un handler de señales como el que teníamos en el TP anterior.