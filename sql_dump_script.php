<?php

date_default_timezone_set('America/Los_Angeles');


class rds
{
    public $conn;

    private $username;
    private $database;
    private $password;
    private $hostname;

    public $increment_mode;
    public $increment_mode_min_rows;
    public $skip_dumps;
    public $range_mode;
    public $other_table_mode;

    public $dump_working_path;
    public $dump_complete_path;
    public $move_dumps;

    public $increment_cache_file;
    public $log_file;
    public $pid_file;

    private $range_tables;
    public  $increment_cache;

    public function __construct()
    {
        $config = parse_ini_file("config.ini");
        $this->username = $config['username'];
        $this->hostname = $config['hostname'];
        $this->password = $config['password'];
        $this->database = $config['database'];

        $this->increment_mode = $config['increment_mode'];
        $this->increment_mode_min_rows = $config['increment_mode_min_rows'];
        $this->range_mode = $config['range_mode'];
        $this->other_table_mode = $config['other_table_mode'];

        $this->dump_working_path = $config['dump_working_path'];
        $this->dump_complete_path = $config['dump_complete_path'];

        $this->skip_dumps = $config['skip_dumps'];

        $this->move_dumps = $config['move_dumps'];

        $this->log_file = $this->dump_working_path . "sql_dump.log";
        $this->increment_cache_file = $this->dump_working_path . "increment_table_cache.json";
        $this->pid_file = $this->dump_working_path . "pid_" . getmypid();

        file_put_contents($this->pid_file, date("Y-m-d H:i:s"));

        # Initiate database connection
        if (! $this->conn = new mysqli($this->hostname, $this->username, $this->password, $this->database)) {
            die("Connection failed: " . mysqli_connect_error());
        }
    }


    private function getRangeTables()
    {
        if(empty($this->range_tables)) {
            $this->range_tables = json_decode(file_get_contents('default_range_tables.json'), true);
        }
        return $this->range_tables;
    }

    private function updateIncrementalLastMax($table, $last_max)
    {
        $cache = $this->getIncrementCache();
        if (isset($cache[$table])) {
            $cache[$table]['last_max'] = $last_max;
        }
        $this->saveIncrementCache($cache);
        $this->logit("Updating cache for $table to $last_max");
    }

    private function getIncrementCache()
    {
        if(empty($this->increment_cache)) {
            if (!file_exists($this->increment_cache_file)) {
                // Read from template
                $this->increment_cache = json_decode(file_get_contents('default_increment_tables.json'), true);
            } else {
                $this->increment_cache = json_decode(file_get_contents($this->increment_cache_file), true);
            }
        }
        return $this->increment_cache;
    }

    private function saveIncrementCache($cache)
    {
        file_put_contents($this->increment_cache_file, json_encode($cache));
        $this->increment_cache = $cache;
    }

    private function logit($message)
    {
        $message = str_replace(
            [$this->password, $this->username],
            ["xxxxxxx", "someUser"],
            $message);
        $message = "[" . date("Y-m-d H:i:s") . "] " . $message . "\n";
        echo $message;
        file_put_contents($this->log_file, $message, FILE_APPEND);
    }

    private function getAllTables() {
        $tables = [];
        $sql = "show full tables where Table_type = 'BASE TABLE'";
        $q = $this->conn->query($sql);
        while ($row = $q->fetch_row()) {
            $table = $row[0];
            $tables[] = $table;
        }
        return $tables;
    }


    public function getDumps() {
        $dumps = [];

        $all_tables = $this->getAllTables();
        $processed_tables = [];

        # Process Incremental Tables
        foreach ($this->getIncrementCache() as $table => $params) {
            $processed_tables[] = $table;
            if (!in_array($table, $all_tables)) continue;
            if ($this->increment_mode)
            {
                $column = $params['column'];
                $last_max = $params['last_max'];
                $bin_count = $params['bin_count'];

                $ranges = $this->getBinValues($table, $column, $last_max, $bin_count, $this->increment_mode_min_rows);
                foreach ($ranges as $start => $end) {
                    $dumps[] = [
                        "table" => $table,
                        "where" => "--where=\"$column > $start and $column <= $end\"",
                        "create" => $start == 0 ? "" : "--skip-add-drop-table --no-create-info",
                        "type" => "incremental",
                        "last_max" => $end,
                        "filename" => str_pad("i_" . $table, 39, "_", STR_PAD_RIGHT) .
                            str_pad($start,10,"0",STR_PAD_LEFT) . "_" .
                            str_pad($end,10,"0",STR_PAD_LEFT)

                    ];
                }
            }
        }

        # Process Range Tables
        foreach ($this->getRangeTables() as $table => $params) {
            $processed_tables[] = $table;
            if (!in_array($table, $all_tables)) continue;
            if ($this->range_mode) {
                $column = $params['column'];
                $bin_count = $params['bin_count'];
                $ranges = $this->getBinValues($table, $column, 0, $bin_count);

                $digits= 3; // = floor(log(count($ranges), 10)) + 1;
                $i = 0;
                foreach ($ranges as $start => $end) {
                    $i++;
                    $part = str_pad($i, $digits, "0", STR_PAD_LEFT) . "of" . count($ranges);
                    $dumps[] = [
                        "table" => $table,
                        "where" => "--where=\"$column > $start and $column <= $end\"",
                        "create" => $start == 0 ? "" : "--skip-add-drop-table --no-create-info",
                        "type" => "range",
                        "last_max" => $end,
                        "filename" => str_pad("r_" . $table, 32, "_", STR_PAD_RIGHT) .
                            $part . "_" .
                            str_pad($start, 10, "0", STR_PAD_LEFT) . "_" .
                            str_pad($end, 10, "0", STR_PAD_LEFT)
                    ];
                }
            }
        }

        $other_tables = array_diff($all_tables, $processed_tables);

        # Process All Other Tables as one dump
        if ($this->other_table_mode == "1") {
            $dumps[] = [
                "table" => implode(" ", $other_tables),
                "where" => "",
                "create" => "",
                "type" => "normal",
                "filename" => "all_other_" . count($other_tables) . "_tables"
            ];
        }

        # Do other tables individually
        if ($this->other_table_mode == "2")
        {
            foreach($other_tables as $table)
            {
                $dumps[] = [
                    "table" => $table,
                    "where" => "",
                    "create" => "",
                    "type" => "normal",
                    "filename" => "s_" . $table
                ];
            }
        }

        // $this->logit("DUMPS\n" . json_encode($dumps));

        return $dumps;
    }

    public function processDumps($dumps) {
        foreach ($dumps as $dump) {
            $table = $dump['table'];
            $where = $dump['where'];
            $create = $dump['create'];
            $filename = $dump['filename'];
            $command = "mysqldump $where --default-character-set=utf8mb4 --single-transaction " .
                "--host=$this->hostname --user=$this->username --password=$this->password " .
                "$create $this->database $table | gzip > $this->dump_working_path" . "$filename" . ".sql.gz";

            $this->logit("[COMMAND]: $command");
            if (! $this->skip_dumps) {
                exec($command);

                if ($dump['type'] == "incremental") {
                    $this->updateIncrementalLastMax($table, $dump['last_max']);
                }

                if ($this->move_dumps == 1) {
                    exec("mv " . $this->dump_working_path . $filename . ".sql.gz " .
                        $this->dump_complete_path . $filename . ".sql.gz");
                    $this->logit("Moved $filename to $this->dump_complete_path");
                }
            }
        }
    }

    private function getValueAtOffset($table, $column, $start, $offset) {
        $sql = "select $column from $table where $column > $start order by $column limit 1 offset $offset";
        $q = $this->conn->query($sql);
        if ($row = $q->fetch_row()) {
            $value = $row[0];
        } else {
            $value = false;
        }
        // $this->logit($sql . " [" . $value . "]");
        return $value;
    }

    private function getMaxValue($table, $column) {
        $sql = "select max($column) from $table";
        $q = $this->conn->query($sql);
        $row = $q->fetch_row();
        return empty($row[0]) ? 0 : $row[0];
    }


    private function getCountValue($table, $where = "") {
        $sql = "select count(*) from $table $where";
        $q = $this->conn->query($sql);
        $row = $q->fetch_row();
        return $row[0];
    }

    private function getBinValues($table, $column, $start, $offset, $min_rows_last_bin = 0)
    {
        $max_value = $this->getMaxValue($table, $column);
        $count = $this->getCountValue($table);
        $values = [];
        $this_value = $start;

        $this->logit("$table has $count rows with max value of $max_value bin size of $offset");

        while($next_value = $this->getValueAtOffset($table, $column, $this_value, $offset)) {
            // $this->logit("$table starting at $this_value returned $next_value with offset $offset");
            $values[$this_value] = $next_value;
            $this_value = $next_value;
        }

        // Check to see if we should include the last bunch
        if ($min_rows_last_bin > 0) {
            // Get number of rows in the last range
            $range_count = $this->getCountValue($table, " where $column > $this_value and $column <= $max_value");
            if ($range_count > $min_rows_last_bin) {
                $values[$this_value] = $max_value;
            } else {
                $this->logit("Skipping $range_count values over $this_value for $table because less than min_rows_last_bin $min_rows_last_bin");
            }
        } else {
            // Always get the small remainder regardless of size
            $values[$this_value] = $max_value;
        }
        return $values;
    }


}


$rds = new rds();

$dumps = $rds->getDumps();

$rds->processDumps($dumps);

//var_dump($dumps);

echo "Done";

/*

# Get All Tables and skip VIEWS


# Build the dumps to be performed as part of this operation
$dumps = [];
foreach ($tables as $table) {
    if (in_array($table, $pid_segment_tables)) {
        // We want to break REDCap data and other pid-tables into multiple segments to speed up parallel uploading so
        // I'm going to hack in a where clause for the redcap_data queries...


}

    $sql2 = "select max(project_id) from $table";
    $q2 = $conn->query($sql2);
    $row = $q2->fetch_row();
    $max_pid = $row[0];
    $pid_bin_size=500; // Break project exports by pid into bins of this range
    $range_start=0;
    $part=1;
    while ($range_start <= $max_pid) {
        $next_range_start = $range_start + $pid_bin_size;
        $tables[] = $table . "|" .
            str_pad($part,3,"0",STR_PAD_LEFT) . "|" .
            "project_id >= $range_start and project_id < $next_range_start";
        $range_start = $next_range_start;
        $part++;
    }
} else {



//var_dump($tables);

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

            if($row_count < $min_rows_for_increment) {
                logit("Skipping chunked table $table - only $row_count new rows which is less than $min_rows_for_increment\n", $log_file);
                continue;
            }

            logit("Dumping $row_count rows from $table where $last_max < $column <= $max\n", $log_file);
            $where = "--where=\"$column > $last_max AND $column <= $max\"";

            if ($last_max > 0) $skipCreate = "--skip-add-drop-table --no-create-info";

            $inc_row_count = $inc_row_count + $row_count;

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
        logit("Dumping $table ...\n", $log_file);
        if(strpos($table, "|") !== false) {
            // We have an embedded where clause
            list($table,$part,$filter) = explode("|",$table,3);
            $where = "--where=\"$filter\"";
            $skipCreate = $part == 1 ? "" : "--skip-add-drop-table --no-create-info";
            $table_filename = $table."_part".$part;
        } else {
            $table_filename = $table;
        }

        $dest_name = "$table_filename.sql.gz";
    }

    exec("mysqldump $where --default-character-set=utf8mb4 --single-transaction --host=$hostname --user=$username --password=$password $skipCreate $database $table | gzip > $destination_path$dest_name");
    file_put_contents($increment_cache_file, json_encode($cache));

    // Move the completed file to a subdir
    exec("mv $destination_path$dest_name $move_when_done_path$dest_name");
    logit("Moved $dest_name to $move_when_done_path\n", $log_file);

}

unlink($pid_file);

logit("----DONE---- ($inc_row_count incremental rows)\n", $log_file);
echo "\n";

*/