<?php

$config = parse_ini_file("config.ini");
$username = $config['username'];
$hostname = $config['hostname'];
$password = $config['password'];
$database = $config['database'];
$increment_only = $config['increment_only'];
$destination_path = $config['destination_path'];

$cache_file = "chunked_tables.json";


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

echo "Found " . count($tables) . " tables - " . count($cache) . " are chunked\n";

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
            echo "Dumping $table from $column >= $last_max to $max\n";
            $where = "--where=\"$column > $last_max AND $column <= $max\"";

            if ($last_max > 0) $skipCreate = "--skip-add-drop-table --no-create-info";

            $dest_name = "$table" . "_" . "$last_max" . "_" . "$max.sql.gz";
            // Update cache
            $cache[$table]['last_max'] = $max;
        } else {
            echo "Skip chunked table $table - no new entries\n";
            continue;
        }
    } else {
        // Dump entire table
        if ($increment_only == 1) {
            echo "Skipping $table - increment only\n";
            continue;
        }
        echo "Dumping $table in entirety\n";
        $dest_name = "$table.sql.gz";
    }

    exec("mysqldump $where --host=$hostname --user=$username --password=$password $skipCreate $database $table | gzip > $destination_path$dest_name");
}

// Save cache
file_put_contents($cache_file, json_encode($cache));
