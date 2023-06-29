const AssignmentEventType = {
	REGISTERED: 1,
	IN_WORK: 2,
	CLOSED: 3,
	ACTION: 4,
	RE_REGISTERED: 5,
	SYSTEM_ACTION: 6,
	CLOSED_AUTO: 7,
}

const AssignmentStatus = {
	REGISTERED: 1,
	IN_WORK: 2,
	CLOSED: 3,
	RE_REGISTERED: 4,
	CLOSED_AUTO: 5,
}


module.exports = {
	AssignmentEventType: AssignmentEventType,
	AssignmentStatus: AssignmentStatus
}