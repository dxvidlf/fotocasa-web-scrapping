import os
import time
import subprocess
from utils.check_global_status import check_global_status
from utils.ensure_base_db_structure import ensure_base_db_structure

ensure_base_db_structure()

start_time = time.time() 

while True:

    process = subprocess.Popen(["python", "main.py"])
    
    while True:
        time.sleep(60)
        try:
            if process.poll() is not None:
                print("✅ main.py terminó por sí solo.")
                break
            last_heartbeat = os.path.getmtime(os.path.join(os.getcwd(), 'status', "heartbeat.txt"))
            if time.time() - last_heartbeat > 30:  # 30 segundos sin señal
                print("⚠️ No hay heartbeat. Matando proceso.")
                process.kill()
                process.wait()
                break
        except Exception as e:
            print(f"⚠️ Error leyendo heartbeat: {e}")
            process.kill()
            process.wait()
            break

    if check_global_status():
        end_time = time.time()
        elapsed_time = end_time - start_time
        minutes, seconds = divmod(int(elapsed_time), 60)
        print(f"✅ Proceso global completo. Duración total: {minutes} minutos y {seconds} segundos.")
        break