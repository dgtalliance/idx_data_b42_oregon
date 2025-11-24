<?php

require_once('GlobalVariables.php');

class DbConnection
{

    private $variables;

    public function __construct()
    {
        $this->variables = new GlobalVariables;
    }

    public function WorkaConnection()
    {
        try {
            $host = $this->variables->workaHost;
            $dbName = $this->variables->activeDb;
            $dbPort = $this->variables->workaPort;
            $dbCharset = $this->variables->dbCharset;
            $dsn = "mysql:host=$host;dbname=$dbName;port=$dbPort;charset=$dbCharset";
            $pdoConnection = new PDO($dsn, $this->variables->workaUser, $this->variables->workaPassword, $this->variables->options);

            return $pdoConnection;
        } catch (\Throwable $th) {
            print("The connection to the database has failed " . $th->getMessage());
        }
    }

    public function ActiveConnection()
    {
      try {
        $host = "104.238.130.255";
        $dbName = $this->variables->activeDb;
        $dbPort = $this->variables->dbPort;
        $dbCharset = $this->variables->dbCharset;
        $dsn = "mysql:host=$host;dbname=$dbName;port=$dbPort;charset=$dbCharset";
        $pdoConnection = new PDO($dsn, "idxwriter", "7EFfYvXE94QDPDbH", $this->variables->options);

        return $pdoConnection;
      }
      catch (\Exception $th) {
        print("The connection to the database has failed " . $th->getMessage());
      }
    }

    public function ClosedConnection()
    {
        try {
            $host = $this->variables->dbHost;
            $dbName = $this->variables->historyDb;
            $dbPort = $this->variables->dbPort;
            $dbCharset = $this->variables->dbCharset;
            $dsn = "mysql:host=$host;dbname=$dbName;port=$dbPort;charset=$dbCharset";
            $pdoConnectionSoldProperties = new PDO($dsn, $this->variables->dbUser, $this->variables->dbPassword, $this->variables->options);

            return $pdoConnectionSoldProperties;
        } catch (\Exception $th) {
            print("The connection to the database has failed " . $th->getMessage());
        }
    }

    public function connectionForPropertiesCpanel()
    {
        try {
            $host = "idxboost.cfylyxyl1g8c.us-east-1.rds.amazonaws.com";
            $dbName = "flexidx_cpanel";
            $dbPort = 3306;
            $dbCharset = $this->variables->dbCharset;
            $dsn = "mysql:host=$host;dbname=$dbName;port=$dbPort;charset=$dbCharset";
            return new PDO($dsn, "root", 'v#R5GyE*^WjXfJ#v$sD', $this->variables->options);
        } catch (\Exception $th) {
            print("The connection to the database has failed " . $th->getMessage());
        }
    }

    public function connectionForCoordinatesControl()
    {
        try {
            $host = "idxboost-prod-nyc3-64557-do-user-1340462-0.a.db.ondigitalocean.com";
            $dbName = "gmaps_api_control";
            $dbPort = $this->variables->dbPort;
            $dbCharset = $this->variables->dbCharset;
            $dsn = "mysql:host=$host;dbname=$dbName;port=$dbPort;charset=$dbCharset";
            return new PDO($dsn, "apictrol", 'Gmap*Apictrl589.', $this->variables->options);
        } catch (\Exception $th) {
            print("The connection to the database has failed " . $th->getMessage());
        }
    }

}