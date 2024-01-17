select
    successfactors_picklistoption.id as picklist_id,
    successfactors_picklistoption.localelabel as picklist_desc
from
    {{source('successfactors_ods','successfactors_picklistoption')}} as successfactors_picklistoption
