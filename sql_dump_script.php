<?php

date_default_timezone_set('America/Los_Angeles');

$config = parse_ini_file("config.ini");
$username = $config['username'];
$hostname = $config['hostname'];
$password = $config['password'];
$database = $config['database'];
$increment_only = $config['increment_only'];


$destination_path = $config['destination_path'];
$move_when_done_path = $config['move_when_done_path'];


$log_file = $destination_path . "sql_dump.log";
$cache_file = $destination_path . "chunked_tables.json";
$pid_file = $destination_path . "pid_" . getmypid();

file_put_contents($pid_file, date("Y-m-d H:i:s"));

$pid_segment_tables = ['redcap_data', 'redcap_metadata_archive'];


/**
 * @param $message
 * @return null
 */
function logit($message, $log_file) {
    $message = "[" . date("Y-m-d H:i:s") . "] " . $message;
    echo $message;
    file_put_contents($log_file, $message, FILE_APPEND);
}



if (file_exists($cache_file)) {
    $cache = json_decode(file_get_contents($cache_file),true);
} else {
    // Initialize
    $chunked_tables = [
        'redcap_log_event' => 'log_event_id',
        'redcap_log_event2' => 'log_event_id',
        'redcap_log_event3' => 'log_event_id',
        'redcap_log_event4' => 'log_event_id',
        'redcap_log_event5' => 'log_event_id',
        'redcap_log_event6' => 'log_event_id',
        'redcap_log_event7' => 'log_event_id',
        'redcap_log_event8' => 'log_event_id',
        'redcap_log_event9' => 'log_event_id',
        'redcap_log_view' => 'log_view_id',
        'redcap_log_view_old' => 'log_view_id',
        'redcap_outgoing_email_sms_log' => 'email_id',
        'redcap_log_api_custom' => 'log_id'
    ];
    $cache = [];
    foreach ($chunked_tables as $table=>$column) {
        $cache[$table] = [
            'column' => $column,
            'last_max' => 0
        ];
    }
    file_put_contents("chunked_tables.json", json_encode($cache));
}




# Initiate database connection
if (!$conn = new mysqli($hostname, $username, $password, $database)) {
    die("Connection failed: " . mysqli_connect_error());
}


# Get All Tables and skip VIEWS
$tables = [];
$sql = "show full tables where Table_type = 'BASE TABLE'";

$include_redcap_data = false;

$q = $conn->query($sql);
while ($row = $q->fetch_row()) {
    $table = $row[0];
    if (in_array($table, $pid_segment_tables)) {
        // We want to break REDCap data and other pid-tables into multiple segments to speed up parallel uploading so
        // I'm going to hack in a where clause for the redcap_data queries...
        $sql2 = "select max(project_id) from $table";
        $q2 = $conn->query($sql2);
        $row = $q2->fetch_row();
        $max_pid = $row[0];
        $pid_bin_size=1000; // Break project exports by pid into bins of this range
        $range_start=0;
        $part=1;
        while ($range_start <= $max_pid) {
            $next_range_start = $range_start + $pid_bin_size;
            $tables[] = $table . "|$part|project_id >= $range_start and project_id < $next_range_start";
            $range_start = $next_range_start;
            $part++;
        }
    } else {
        // Temp for andy
        //$tables[] = $table;
    }
}

var_dump($tables);

logit("Found " . count($tables) . " tables parts to export - " . count($cache) . " are chunked\n", $log_file);

if ($increment_only == 1) {
    logit("Only dumping incremental tables\n", $log_file);
}


# We want to break redcap_data into multiple smaller files as it is so large
foreach ($tables as $table) {
    $skipCreate="";
    $where="";
    if (isset($cache[$table])) {
        // Chunked
        $column = $cache[$table]['column'];
        $last_max = $cache[$table]['last_max'];
        $sql = "select max($column) from $table";
        $q = $conn->query($sql);
        $max = $q->fetch_row()[0];
        if ($max > $last_max) {
            // calculate step and only jump by step or 1M in next round
            $tenth = floor($max/20);
            $step = max($tenth,1000000);
            $max = min($last_max + $step, $max);
            $row_count = $max - $last_max;

            logit("Dumping $row_count rows from $table where $last_max < $column <= $max\n", $log_file);
            $where = "--where=\"$column > $last_max AND $column <= $max\"";

            if ($last_max > 0) $skipCreate = "--skip-add-drop-table --no-create-info";


            $dest_name = "i_$table" . "_" .
                str_pad($last_max,12,"0",STR_PAD_LEFT) . "_" .
                str_pad($max,12,"0",STR_PAD_LEFT) . ".sql.gz";
            // Update cache
            $cache[$table]['last_max'] = $max;
        } else {
            logit("Skip chunked table $table - no new entries\n", $log_file);
            continue;
        }
    } else {
        // Dump entire table
        if ($increment_only == 1) {
            //echo "Skipping $table - increment only\n";
            continue;
        }
        logit("Dumping $table in entirety\n", $log_file);
        if(strpos($table, "|") !== false) {
            // We have an embedded where clause
            list($table,$part,$filter) = explode("|",$table,3);
            $where = "--where=\"$filter\"";
            $table_filename = $table."_part".$part;
        } else {
            $table_filename = $table;
        }

        $dest_name = "$table_filename.sql.gz";
    }

    exec("mysqldump $where --single-transaction --host=$hostname --user=$username --password=$password $skipCreate $database $table | gzip > $destination_path$dest_name");
    file_put_contents($cache_file, json_encode($cache));

    // Move the completed file to a subdir
    exec("mv $destination_path$dest_name $move_when_done_path$dest_name");
    logit("Moved $dest_name to $move_when_done_path\n", $log_file);

}

unlink($pid_file);


// Save cache
//file_put_contents($cache_file, json_encode($cache));
