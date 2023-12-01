import schedule
import time
import subprocess

# Contador para controlar las iteraciones
iteraciones = 0

def ejecutar_script():
    global iteraciones
    # Reemplaza 'archivo.py' con la ruta de tu archivo.py
    subprocess.run(['python', 'ruta/al/archivo.py'])
    iteraciones += 1
    if iteraciones >= 20:
        # Detener el script después de 20 iteraciones
        print("Se han completado 20 iteraciones. Deteniendo el script.")
        schedule.clear()

# Función para verificar la hora y ejecutar el script si es 22:00
def verificar_hora():
    hora_actual = time.localtime()
    if hora_actual.tm_hour == 22 and hora_actual.tm_min == 0:
        ejecutar_script()

# Programar la verificación cada minuto
schedule.every().minute.do(verificar_hora)

# Bucle para ejecutar la verificación continuamente
while True:
    schedule.run_pending()
    time.sleep(1)
