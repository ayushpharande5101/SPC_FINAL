import time
from pylogix import PLC


while True:
    with PLC() as comm:
        comm.IPAddress = '192.168.10.1'
        print("TRUE")

        TEST_BIT = comm.Read('SPC_PLC_COMMUNICATION_BIT')

        # comm.Write('SPC_PLC_COMMUNICATION_BIT',True)
        Second_Bit = comm.Read('SPC_PLC_COMMUNICATION_BIT_01')

        if Second_Bit.Value == True:
            print("VALUES READABLE")
            time.sleep(1)
            comm.Write('SPC_PLC_COMMUNICATION_BIT',True)

        else:
            print("Values Unreadable")