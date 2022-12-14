from dbhpgm.dbConfig import dbConnector
from dbhpgm.hpgm import hpgm

from input import *

engineCH = dbConnector('chatamart', project_id=project_id1)
HPGM = hpgm()
mappingDF = HPGM.getAliasCondMappingDataframe(project_id=project_id1, mapping_id=mapping_id)

withClause = HPGM.buildWithClause(mappingDF)
multiIf = HPGM.buildMultiIf(mappingDF)

# Idea for segments with different mappings
"""if project_id1 == project_id2:
    mappingDF = HPGM.getAliasCondMappingDataframe(project_id=project_id1, mapping_id=mapping_id)
else:
    mappingDF = HPGM.getAliasCondMappingDataframe(project_id=project_id[0], mapping_id=mapping_id)
    mappingDF2 = HPGM.getAliasCondMappingDataframe(project_id=project_id[1], mapping_id=mapping_id2)"""
