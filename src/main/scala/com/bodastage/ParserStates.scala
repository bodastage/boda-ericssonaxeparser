package com.bodastage

object ParserStates extends Enumeration {
	type ParserStates = Value
	val EXTRACTING_PARAMETERS, EXTRACTING_VALUES = Value
}