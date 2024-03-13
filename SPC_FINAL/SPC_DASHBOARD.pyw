import subprocess

command2 = ["streamlit", "run", "new_2.py"]
process2 = subprocess.Popen(command2)


process2.wait()

print("All programs have finished.")