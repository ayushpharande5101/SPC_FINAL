import subprocess

# Define the commands for each program
command1 = ["python", "COMBINE_5.py"]
process1 = subprocess.Popen(command1)

#command2 = ["streamlit", "run", "new_2.py"]
#process2 = subprocess.Popen(command2)

command3 = ["Python", "Test_Bit.py"]
process3 = subprocess.Popen(command3)

command4 = ["Python", "Report_not_ok_1.py"]
process4 = subprocess.Popen(command4)

command5 = ["Python", "FINAL_REPORT.py"]
process5 = subprocess.Popen(command5)

# Wait for both processes to finish
process1.wait()
# process2.wait()
process3.wait()
process4.wait()
process5.wait()

print("All programs have finished.")
