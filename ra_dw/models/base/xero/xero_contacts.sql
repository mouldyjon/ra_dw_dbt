/*
    The source function accepts two arguments:
      1. The name of the source
      2. The name of the table in that source
*/

select * from {{ source('xero', 'contacts') }}

/*
	This is compiled to:
  
    select * from "target_database"."source_1"."table_1"
    
*/
