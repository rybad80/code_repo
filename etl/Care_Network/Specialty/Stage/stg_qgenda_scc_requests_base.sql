select
	qgenda_request.requestkey,
	qgenda_request.taskid,
	qgenda_request.staffabbrev,
	qgenda_request.staffkey,
	qgenda_request.stafflname,
	qgenda_request.stafffname,
	qgenda_request.taskkey,
	qgenda_request.taskabbrev,
	qgenda_request.taskname,
	qgenda_request.submitteddate,
	qgenda_request.upd_dt
from {{ source('qgenda_ods', 'qgenda_request') }} as qgenda_request
where qgenda_request.compkey = '1121178a-aa59-4654-9160-043975c9fff1'
