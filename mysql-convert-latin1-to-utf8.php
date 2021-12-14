<?php
/**
 * mysql-convert-latin1-to-utf8.php
 *
 * v1.3
 *
 * Converts incorrect MySQL latin1 columns to UTF8MB4 
 * and checks for latin1 stored as latin1
 * and removes any FK's using latin1 columns
 * NOTE: Look for 'TODO's for things you may need to configure.
 *
 * Documentation at:
 *  http://nicj.net/2011/04/17/mysql-converting-an-incorrect-latin1-column-to-utf8
 *
 * Or, read README.md.
 *
 * PHP Version 7.0
 *
 * @author    Nic Jansma <nic@nicj.net>
 * @copyright 2013 Nic Jansma
 * @link      http://www.nicj.net
 */

// TODO: Pretend-mode -- if set to true, no SQL queries will be executed.  Instead, they will only be echo'd
// to the console.
$pretend = true;

// TODO: Should SET and ENUM columns be processed?
$processEnums = false;

// TODO: The collation you want to convert the overall database to
$defaultCollation = 'utf8mb4_unicode_ci';

// TODO: The collation for the client to use during the conversion
$defaultCharset = 'utf8mb4';

// TODO: Convert column collations and table defaults using this mapping
// latin1_swedish_ci is included since that's the MySQL default
$collationMap =
 array(
  'latin1_bin'        => 'utf8mb4_bin',
  'latin1_general_ci' => 'utf8mb4_unicode_ci',
  'latin1_swedish_ci' => 'utf8mb4_unicode_ci'
 );

$dbHost = 'localhost';
$dbName = '';
$dbUser = '';
$dbPass = '';
$dbPort = '';
// where to store the corruptable data
$tmpTable = 'tmp_storage';

// array of tables to process or [] for all tables
$tablesToConvert = [];

if (file_exists('config.php')) {
    require_once('config.php');
}

$mapstring = '';
foreach ($collationMap as $s => $t) {
    $mapstring .= "'$s',";
}

// Strip trailing comma
$mapstring = substr($mapstring, 0, -1);
echo $mapstring . PHP_EOL;

// Open a connection to the information_schema database
$infoDB = new mysqli($dbHost, $dbUser, $dbPass, 'information_schema', $dbPort);
$infoDB->select_db('information_schema');

// Open a second connection to the target (to be converted) database
$targetDB = new mysqli($dbHost, $dbUser, $dbPass, '', $dbPort);
$targetDB->select_db($dbName);
$targetDB->set_charset($defaultCharset);

$createSql = "CREATE TABLE IF NOT EXISTS {$tmpTable}  ( 
    id int primary key auto_increment not null, 
    table_id_name varchar(255),
    table_id varchar(255) not null,
    table_name varchar(255),
    column_name varchar(255),
    binary_value LONGBLOB,
    latin1_value LONGTEXT collate latin1_general_ci,
    utf8_value LONGTEXT collate utf8mb4_unicode_ci,
    insert_date DATETIME
    )";
sqlExec($targetDB, $createSql, false);

// Get all tables in the specified database
$tables = sqlObjs(
    $infoDB,
    "SELECT TABLE_NAME, TABLE_COLLATION
     FROM   TABLES
     WHERE  TABLE_SCHEMA = '$dbName'"
);

foreach ($tables as $table) {
    $tableName      = $table->TABLE_NAME;
    $tableCollation = $table->TABLE_COLLATION;
    if ($tableName == $tmpTable || $tableName=='tmp_storage') {
        continue;
    }
    // if the table is not in the list of tables to convert, skip it
    if($tablesToConvert && !in_array($tableName,$tablesToConvert)) {
        continue;
    }
   
    // Find all columns whose collation is of one of $mapstring's source types
    $cols = sqlObjs(
        $infoDB,
        "SELECT *
         FROM   COLUMNS
         WHERE  TABLE_SCHEMA    = '$dbName'
            AND TABLE_Name      = '$tableName'
            AND COLLATION_NAME IN ($mapstring)
            AND COLLATION_NAME IS NOT NULL"
    );

    $intermediateChanges = array();
    $finalChanges        = array();
 
    $primary_key_search = sqlObjs($infoDB, "SELECT COLUMN_NAME 
            FROM COLUMNS WHERE TABLE_SCHEMA = '$dbName'
            AND TABLE_NAME = '$tableName'
            AND COLUMN_KEY = 'PRI'");
    if ($primary_key_search) {
        $primary_key = $primary_key_search[0]->COLUMN_NAME;
    } else {
        // not suitable for conversion - no primary key
        echo "table " . $tableName . " no primary key skipping. " .PHP_EOL;
        continue;
    }



    foreach ($cols as $col) {
        // If this column doesn't use one of the collations we want to handle, skip it
        if (!array_key_exists($col->COLLATION_NAME, $collationMap)) {
            continue;
        } else {
            $targetCollation = $collationMap[$col->COLLATION_NAME];
            echo "Collation Column Found : {$col->COLLATION_NAME}" . PHP_EOL;
        }

        // Save current column settings
        $colName      = $col->COLUMN_NAME;
        $colCollation = $col->COLLATION_NAME;
        $colType      = $col->COLUMN_TYPE;
        $colDataType  = $col->DATA_TYPE;
        $colLength    = $col->CHARACTER_OCTET_LENGTH;
        $colNull      = ($col->IS_NULLABLE === 'NO') ? 'NOT NULL' : '';

        $colDefault = '';
        if ($col->COLUMN_DEFAULT !== null) {
            $colDefault = "DEFAULT '{$col->COLUMN_DEFAULT}'";
        }

        // Determine the target temporary BINARY type
        $tmpDataType = '';
        switch (strtoupper($colDataType)) {
            case 'CHAR':
                $tmpDataType = 'BINARY';
                break;

            case 'VARCHAR':
                $tmpDataType = 'VARBINARY';
                break;

            case 'TINYTEXT':
                $tmpDataType = 'TINYBLOB';
                break;

            case 'TEXT':
                $tmpDataType = 'BLOB';
                break;

            case 'MEDIUMTEXT':
                $tmpDataType = 'MEDIUMBLOB';
                break;

            case 'LONGTEXT':
                $tmpDataType = 'LONGBLOB';
                break;

            case 'SET':
            case 'ENUM':
                $tmpDataType = 'SKIP';
                if ($processEnums) {
                    $finalChanges[] = "MODIFY `$colName` $colType COLLATE $defaultCollation $colNull $colDefault";
                }
                break;

            default:
                $tmpDataType = '';
                break;
        }

        // any data types marked as SKIP were already handled
        if ($tmpDataType === 'SKIP') {
            continue;
        }

        if ($tmpDataType === '') {
            print "Unknown type! $colDataType\n";
            exit;
        }

        $checkCorruption = "SELECT {$primary_key}
         FROM {$tableName} WHERE 
         CONVERT(CONVERT({$colName} USING BINARY) USING utf8mb4) is null and {$colName} is not null";
        $result = sqlExec($targetDB, $checkCorruption, false);
        // if corruption will occur dump the data
        if ($result && $result->num_rows>0) {
            echo "TABLE " . $tableName . "." . $colName . " has ".$result->num_rows." rows in LATIN1 not UTF8 which will be lost. SAVING THEM" . PHP_EOL;
            $sql = "INSERT INTO {$tmpTable} (table_id_name, table_id, table_name, column_name, binary_value, latin1_value, utf8_value, insert_date) 
                SELECT '{$primary_key}',{$primary_key}, '{$tableName}', '{$colName}', binary({$colName}),
                CONVERT({$colName} USING latin1),
                CONVERT({$colName} USING utf8mb4),
                now() 
                FROM {$tableName}
                WHERE CONVERT(CONVERT({$colName} USING BINARY) USING utf8mb4) is null and {$colName} is not null
            ";
            sqlExec($targetDB, $sql, false, true);
            // now clear the data for restoration later
            $sql = "UPDATE $tableName set {$colName} = ''
            WHERE CONVERT(CONVERT({$colName} USING BINARY) USING utf8mb4) is null and {$colName} is not null
            ";
            sqlExec($targetDB, $sql, $pretend, true);
            
        }
        // check if the data is in a foreign key
        $a = sqlObjs($infoDB, "SELECT * FROM key_column_usage  WHERE TABLE_SCHEMA = '{$dbName}'  AND
        TABLE_NAME = '{$tableName}' AND COLUMN_NAME = '{$colName}'");
        if ($a) {
            sqlExec($targetDB, "ALTER TABLE {$tableName} DROP FOREIGN KEY {$a[0]->CONSTRAINT_NAME}", $pretend, true);
        }

        // Change the column definition to the new type
        $tempColType = str_ireplace($colDataType, $tmpDataType, $colType);

        // Convert the column to the temporary BINARY cousin
        $intermediateChanges[] = "MODIFY `$colName` $tempColType $colNull";

        // Convert it back to the original type with the correct collation
        $finalChanges[] = "MODIFY `$colName` $colType COLLATE $targetCollation $colNull $colDefault";
    }

    if (array_key_exists($tableCollation, $collationMap)) {
        $finalChanges[] = 'DEFAULT COLLATE ' . $collationMap[$tableCollation];
    }

    // Now run the conversions
    if (count($intermediateChanges) > 0) {
        sqlExec($targetDB, "ALTER TABLE `$dbName`.`$tableName`\n". implode(",\n", $intermediateChanges), $pretend, true);
    }

    if (count($finalChanges) > 0) {
        sqlExec($targetDB, "ALTER TABLE `$dbName`.`$tableName`\n". implode(",\n", $finalChanges), $pretend, true);
    }
}
//
// TODO: Restore FULLTEXT indexes here
// eg.
//    sqlExec($targetDB, "ALTER TABLE MyTable ADD FULLTEXT KEY `my_index_name` (`mycol1`)", $pretend);
// Set the default collation
sqlExec($infoDB, "ALTER DATABASE `$dbName` COLLATE $defaultCollation", $pretend);

restoreData($targetDB, $tmpTable, $pretend);
// Done!

//
// Functions
//
/**
 * Executes the specified SQL
 *
 * @param object  $db      Target SQL connection
 * @param string  $sql     SQL to execute
 * @param boolean $pretend Pretend mode -- if set to true, don't execute query
 *
 * @return SQL result
 */
function sqlExec($db, $sql, $pretend = false, $echo = false)
{
    if ($echo) {
        echo "$sql;\n";
    }
    $res = null;
    if ($pretend === false) {
        $res = $db->query($sql);
        if ($res === false) {
            $error = $db->error_list[0]['error'];
            print "!!! ERROR: $error\n";
            print "!!! SQL: $sql\n";

        }
    }
    return $res;
}

/**
 * Gets the SQL back as objects
 *
 * @param object $db  Target SQL connection
 * @param string $sql SQL to execute
 *
 * @return SQL objects
 */
function sqlObjs($db, $sql)
{
    $res = sqlExec($db, $sql);

    $a = array();

    if ($res !== false) {
        while ($obj = $res->fetch_object()) {
            $a[] = $obj;
        }
    }

    return $a;
}

/**
 * Undocumented function
 *
 * @param object $db Target SQL connection
 * @param integer $pretend flag as to whether to execute or not
 *
 * @return void
 */
function restoreData($db, $tmpTable, $pretend)
{
    // recover data
    $restoreData = "SELECT table_name FROM `{$tmpTable}` group by table_name";
    $tables = sqlExec($db, $restoreData, false);
    if ($tables && $tables->num_rows>0) {
        foreach ($tables as $table) {
            $sql = "SELECT id, table_id_name, table_name, column_name FROM `{$tmpTable}` 
                where table_name = '{$table['table_name']}'";
            $result = sqlExec($db, $sql, false);
            foreach ($result as $value) {
                $record = $value['id'];
                $table_id_name = $value['table_id_name'];
                $table_name = $value['table_name'];
                $column_name = $value['column_name'];
                $sql = "UPDATE `{$table_name}` a, `{$tmpTable}` t set a.`{$column_name}` = t.`utf8_value` where 
                        t.`id` = {$record} and a.`{$table_id_name}` = t.`table_id` ";
                sqlExec($db, $sql, $pretend, true);
            }
        }
    }
}

