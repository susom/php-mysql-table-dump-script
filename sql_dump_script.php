<?php

date_default_timezone_set('America/Los_Angeles');

$config = parse_ini_file("config.ini");
$username = $config['username'];
$hostname = $config['hostname'];
$password = $config['password'];
$database = $config['database'];
$increment_only = $config['increment_only'];
$destination_path = $config['destination_path'];
$log_file = $destination_path . "sql_dump.log";

$cache_file = $destination_path . "chunked_tables.json";

$pid_file = $destination_path . "pid_" . getmypid();

file_put_contents($pid_file, date("Y-m-d H:i:s"));

/**
 * @param $message
 * @return null
 */
function logit($message, $log_file) {
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


# Get All Tables
$tables = [];
$sql = "show tables from redcap";
$q = $conn->query($sql);
while ($row = $q->fetch_row()) {
    $tables[] = $row[0];
}

logit("Found " . count($tables) . " tables - " . count($cache) . " are chunked\n", $log_file);

if ($increment_only == 1) {
    logit("Only dumping incremental tables\n", $log_file);
}

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


            $dest_name = "$table" . "_" .
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
        $dest_name = "$table.sql.gz";
    }

    exec("mysqldump $where --host=$hostname --user=$username --password=$password $skipCreate $database $table | gzip > $destination_path$dest_name");

    file_put_contents($cache_file, json_encode($cache));
}

unlink($pid_file);


// Save cache
//file_put_contents($cache_file, json_encode($cache));
