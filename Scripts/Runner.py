import Shadow
import time

SAVE_AFTER_ITR = 2
WAIT_RANGE = (10, 12)
URL_JUMP_LAG = (2, 4)
NUM_RETRIES = 1

# Reset the value to True if the last run has failed or to retry failed keywords with a different domain
CONTINUE_OR_RETRY = False

print('Started @', time.strftime("%a %H:%M:%S"))

Shadow.activate_bot(SAVE_AFTER_ITR, URL_JUMP_LAG, NUM_RETRIES, WAIT_RANGE, CONTINUE_OR_RETRY)

print('Done @', time.strftime("%a %H:%M:%S"))
