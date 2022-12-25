import utils
import config

'''
rcStart, rcEnd = utils.getDatetime(30, 0)
print(rcStart, rcEnd)

for site in config.SITES:
    print(f"{site}:", utils.getCount(site, config.rcTag, rcStart, rcEnd))
'''

utils.backfill(1, 30)
