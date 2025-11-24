<?php

use GuzzleHttp\Client;
use Gumlet\ImageResize;
use Elastic\Elasticsearch\ClientBuilder;

class Helpers {

  private Client $clientGuzzle;

  private $client;

  private $connection;

  private $globalVariables;

  private $logger;

  private $shards;

  private $replicas;

  private $index;

  private $workaTable;

  private $table;

  public function __construct($status = 'active') {
    $this->connection = new DbConnection();
    $this->logger = new Logger();
    $this->globalVariables = new GlobalVariables();
    $this->clientGuzzle = new Client();
    if ($status == 'closed') {
      $this->client = ClientBuilder::create()
        ->setHosts(['https://es-cluster-sold-vbxotcyc-y3xzm9uqi6.onidxboost.com'])
        ->setBasicAuthentication('elastic', 'EMhwFxYNr622zYgG')
        ->build();
    }
    else {
      $this->client = ClientBuilder::create()
        ->setHosts(['https://es-cluster-vbxotcyc.onidxboost.com'])
        ->setBasicAuthentication('elastic', '6P_L*FWrAkw2eOejwn*+')
        ->build();
    }
    $this->shards = 3;
    $this->replicas = 2;
  }

  public
  function setIndex($board, $status, $year = NULL
  ) {
    $this->status = $status;
    $this->index = "index_" . $status . "_" . $board;
    if ($year != NULL) {
      $this->index = $this->index . "_" . $year;
      $this->year = $year;
    }

    if (!preg_match('/active/i', $status)) {
      $this->workaTable = 'Closed_Property';
      $this->table = 'idx_property_sold_rented';
    }
    else {
      $this->workaTable = 'Active_Property';
      $this->table = 'idx_property_active_pending';
    }
  }

  function insertToAgents($agents, $con) {
    if (isset($agents['values']) && count($agents['values']) > 0) {
      foreach ($agents['values'] as $agent) {
        $agentCode = (isset($agent[4])) ? "'" . addslashes($agent[4]) . "'" : 'NULL';
        $agentName = (isset($agent[0])) ? "'" . addslashes($agent[0]) . "'" : 'NULL';
        $agentEmail = (isset($agent[1])) ? "'" . addslashes($agent[1]) . "'" : 'NULL';
        $agentPhone = (isset($agent[2])) ? "'" . addslashes($agent[2]) . "'" : 'NULL';
        $agentFax = (isset($agent[8])) ? "'" . addslashes($agent[8]) . "'" : 'NULL';
        $agentState = (isset($agent[3])) ? "'" . addslashes($agent[3]) . "'" : 'NULL';
        $creationDate = new DateTime();
        $creationDate = "'" . $creationDate->format('Y-m-d H:i:s') . "'";
        $agentsString[] = "(0,$agentCode,$agentName,$agentEmail,$agentPhone,$agentFax,$agentState,$creationDate,21)";
        IdxLogger::setLog("Added to the package agent with ID $agentCode", IdxLog::type_success);
      }
      //                var_dump($agentsString);die;
    }
    $cant = count($agentsString);
    $implodedallAgents = implode(',', $agentsString);

    return $this->InsertAgents($implodedallAgents, $cant, $con);
  }

  public function errorlog(Exception $e, $sql = NULL) {
    $this->logger->errorLog($e->getCode());
    $this->logger->errorLog($e->getMessage());
    if ($sql != NULL) {
      $this->logger->errorLog($sql);
    }
    die;
  }

  public function InsertAgents($implodedString, $cant, $con) {
    $sqlString = "INSERT IGNORE INTO idx_agent(mui,code,name,email,phone,fax,state,create_at,source_id) VALUES $implodedString";
    IdxLogger::setLog("Inserted $cant Agents", IdxLog::type_success);
    return $con->prepare($sqlString)->execute();
  }

  function insertToOffices($offices, $con) {
    if (isset($offices['values'])) {
      foreach ($offices['values'] as $office) {
        $officeCode = (isset($office[2])) ? "'" . addslashes($office[2]) . "'" : 'NULL';;
        $officeName = (isset($office[0])) ? "'" . addslashes($office[0]) . "'" : 'NULL';;
        $officePhone = (isset($office[1])) ? "'" . addslashes($office[1]) . "'" : 'NULL';;
        $creationDate = new DateTime();
        $creationDate = "'" . $creationDate->format('Y-m-d H:i:s') . "'";
        $officesString[] = "(0,$officeCode,$officeName,$officePhone,$creationDate)";

        IdxLogger::setLog("Added to the package office with ID $officeCode", IdxLog::type_success);
      }
    }
    $cant = count($officesString);
    $implodedOfficesArray = implode(',', $officesString);
    return $this->InsertOffices($implodedOfficesArray, $con, $cant);
  }

  public function InsertOffices($implodedString, $con, $cant) {
    $sqlString = "INSERT IGNORE INTO idx_office(mui,code,name,phone,create_at) VALUES $implodedString";
    return $con->prepare($sqlString)->execute();
    IdxLogger::setLog("Inserted $cant Offices", IdxLog::type_success);
  }

  function logTimer($initTime) {
    $endTime = date("H:i:s");
    $hi = new DateTime($initTime);
    $hf = new DateTime($endTime);
    $interval = $hi->diff($hf);
    $duration = $interval->format('Duration of the process: %H horas %i minutos %s segundos');
    $this->logger->confirmationLog($duration);
  }

  public function checkEmptyValue($variableToCheck) {
    if (is_numeric($variableToCheck)) {
      return (!empty($variableToCheck)) ? $variableToCheck : 0;
    }
    elseif (is_string($variableToCheck)) {
      $variableToCheck = str_replace('"', "'", $variableToCheck);
      return (!empty($variableToCheck) && is_string($variableToCheck)) ? '"' . $variableToCheck . '"' : '';
    }
    else {
      return 'NULL';
    }
  }

  public function saveGeocodeFromBackup($table, $con) {
    $offset = 0;
    do {
      $sql = "Select * from GeocodeBackup Limit $offset,1000";
      $data = $con->query($sql)->fetchAll();

      foreach ($data as $value) {
        $value['address'] = addslashes($value['address']);
        $sql = "Update $table set Latitude={$value['Latitude']},Longitude={$value['Longitude']} where UnparsedAddress like '%{$value['address']}'";
        $con->prepare($sql)->execute();
        var_dump($sql);
        $offset += 1000;
        IdxLogger::setLog("Updated Proeprty with address {$value['address']}");
      }
    }
    while (count($data) == 1000);
  }

  function getLastUpdateByEndpoint($table) {
    $lastUpdate = $this->getLastUpdateProperties($table);
    if ($lastUpdate == NULL) {
      $currentDate = date('d-m-Y');
      $temp = strtotime('- 2 days', strtotime($currentDate));
      $currentDateFormat = date("Y-m-d", $temp) . "00:00:00";
      $final = DateTime::createFromFormat("Y-m-d H:i:s", $currentDateFormat);

      $lastUpdate = str_replace(" ", 'T', $final->format(DateTime::RFC3339));
      $lastUpdate = str_replace("-05:", "+00:", $lastUpdate);
      $lastUpdate = str_replace("+00:00", "Z", $lastUpdate);
    }
    else {
      $lastUpdate = str_replace("+", "-", $lastUpdate);

      $lastUpdate = strtotime('- 1 hours', strtotime($lastUpdate));
      $lastUpdate = date("Y-m-d H:i:s", $lastUpdate) . "-00:00";
      $lastUpdate = str_replace(" ", 'T', $lastUpdate);

      $lastUpdate = str_replace("-05:", "+00:", $lastUpdate);
      $lastUpdate = str_replace("-00:00", "Z", $lastUpdate);
    }

    return $lastUpdate;
  }

  function getLastUpdateProperties($table) {
    $sql = "SELECT ModificationTimestamp FROM $table ORDER BY ModificationTimestamp desc LIMIT 1";
    $result = $this->connection->WorkaConnection()->query($sql)->fetch();
    $lastUpdated = isset($result['ModificationTimestamp']) ? $result['ModificationTimestamp'] : NULL;
    return $lastUpdated;
  }

  function getLastUpdatePropertiesAP($con, $table) {
    $sql = "SELECT last_updated FROM $table  ORDER BY last_updated desc LIMIT 1";
    $result = $con->query($sql)->fetch(PDO::FETCH_ASSOC);
    $lastUpdated = isset($result['last_updated']) ? $result['last_updated'] : NULL;
    return $lastUpdated;
  }

  public function getAllPropertiesToUpdate($status, $nextLink = NULL) {
    $filter = "";
    $lastUpdateQueryString = '';
    $accessToken = $this->globalVariables->accessTokenForProperties;
    $metaDataEndPoint = $this->globalVariables->georgiaMetaDataEndpoint;
    if ($status != "C") {
      $filter = "StandardStatus eq 'Pending' or StandardStatus eq 'Active' or StandardStatus eq 'Active Under Contract'";
    }
    else {
      $filter = "StandardStatus eq 'Closed' and CloseDate gt 2017-01-01";
    }

    if ($nextLink != NULL) {
      $getAllActiveProperties = $this->client->request('GET', $nextLink);
    }
    else {
      $getAllActiveProperties = $this->client->request('GET', "$metaDataEndPoint/tmls/Property/replication?access_token=$accessToken&" . '$filter' . "=$filter" . '&$top=2000&$select=ListOfficeMlsId,CoListOfficeMlsId,ListingKeyNumeric');
    }

    if ($getAllActiveProperties->getStatusCode() == 200) {
      return json_decode($getAllActiveProperties->getBody()
        ->getContents(), TRUE);
    }
    else {
      return FALSE;
    }
  }

  function login() {
    $client = new \GuzzleHttp\Client(['cookies' => TRUE]);
    $jar = new \GuzzleHttp\Cookie\CookieJar();

    $client->request('GET', "{$this->globalVariables->endpointMetadata}/login", [
      'auth' => [
        $this->globalVariables->user,
        $this->globalVariables->pass,
        'digest',
      ],
      'headers' => ['RETS-Version' => "RETS/1.7"],
      'cookies' => $jar,
    ]);
    $result = [
      'AWSALB' => $jar->getCookieByName('AWSALB')->getValue(),
      'AWSALBCORS' => $jar->getCookieByName('AWSALBCORS')->getValue(),
      'JSESSIONID' => $jar->getCookieByName('JSESSIONID')->getValue(),
    ];

    return $result;
  }

  function getAllActivePendingProperties($status, $lastUpdate = NULL) {
    $token = $this->globalVariables->TOCKEN;
    $select = $this->globalVariables->selectdla;

    // Construcción del filtro
    if ($status === "Closed") {
      $filter = "MlsStatus in ('Sold','Leased') and CloseDate ge 2019-07-01";
    }
    else {
      $filter = "(StandardStatus+eq+Odata.Models.StandardStatus%27Active%27+or+StandardStatus+eq+Odata.Models.StandardStatus%27Pending%27+or+StandardStatus+eq+Odata.Models.StandardStatus%27ActiveUnderContract%27)";
    }


    // Agregar condición de fecha si existe
    if ($lastUpdate !== NULL) {
      $filter .= "%20and%20ModificationTimestamp%20ge%20$lastUpdate";
    }

    // Armado del URL
    $url = 'https://resoapi.rmlsweb.com/reso/odata/Property?$filter=' . $filter . '&$top=100&$orderby=ModificationTimestamp&$select=' . $select;

    $response = $this->clientGuzzle->request('GET', $url, [
      'headers' => [
        'Authorization' => $token,
        'Accept' => 'application/json',
      ],
    ]);

    if ($response->getStatusCode() === 200) {
      return json_decode($response->getBody()->getContents(), TRUE);
    }

    return FALSE;
  }

  function getAllActivePendingPropertiestodelete($lastUpdate) {
    $token = $this->globalVariables->VOW_TOCKEN;
    $select = 'ListingKey,ModificationTimestamp';

    $filter = "MlsStatus in ('Extension','Sold Conditional','Sold Conditional Escape','Draft','Leased Conditional Escape','Price Change','New') and ModificationTimestamp ge $lastUpdate";

    $url = 'https://query.ampre.ca/odata/Property?$filter=' . $filter . '&$top=1000&$orderby=ModificationTimestamp&$select=' . $select;

    $response = $this->clientGuzzle->request('GET', $url, [
      'headers' => [
        'Authorization' => $token,
        'Accept' => 'application/json',
      ],
    ]);

    if ($response->getStatusCode() === 200) {
      return json_decode($response->getBody()->getContents(), TRUE);
    }

    return FALSE;
  }

  function deleteNonComingProperties($propertiesIds) {
    $propertiesToDelete = implode("','", $propertiesIds);

    $sql = $this->connection->WorkaConnection()
      ->prepare("DELETE FROM Active_Property WHERE ListingKey NOT IN('$propertiesToDelete')");
    $sql->execute();
    $this->logger->errorLog("Delete properties in Worka Finished");
  }

  public function getAndParseXml($response) {
    $data = new SimpleXMLElement($response);
    $xmlObject = json_encode((array) $data);
    $result = (array) json_decode($xmlObject);

    $DATA = isset($result['DATA']) ? $result['DATA'] : NULL;
    $COLUMNS = isset($result['COLUMNS']) ? $result['COLUMNS'] : NULL;
    $COLUMNS = explode("\t", $COLUMNS);

    unset($COLUMNS[count($COLUMNS) - 1]);
    unset($COLUMNS[0]);
    $arrayPropertyWorka = [];
    $arrayColumns = [];
    if (is_array($DATA)) {
      for ($i = 0; $i < count($DATA); $i++) {
        $datos = explode("\t", $DATA[$i]);
        for ($j = 1; $j <= count($COLUMNS); $j++) {
          $arrayPropertyWorka[$i][] = trim($datos[$j]);
        }
      }
    }
    else {
      $datos = explode("\t", $DATA);
      for ($j = 1; $j <= count($COLUMNS); $j++) {
        $arrayPropertyWorka[0][] = trim($datos[$j]);
      }
    }

    $return['values'] = $arrayPropertyWorka;
    $return['col'] = $COLUMNS;

    return $return;
  }

  public function getAndParseCsv($handle) {
    $data = explode("\n", $handle);

    $col = trim($data[0]);
    $col = str_replace('"', '', $col);

    $explodeCol = explode(',', $col);
    unset($data[(count($data) - 1)]);
    unset($data[0]);
    $result = [];
    foreach ($data as $key1 => $value) {
      $test = explode('","', $value);
      foreach ($explodeCol as $key => $item) {
        $result[$key1][$item] = trim(str_replace('"', '', $test[$key]));
      }
    }
    return $result;
  }

  function updateAgentAndOffices($data) {
    $conection = $this->connection->ActiveConnection();

    foreach ($data as $property) {
      $agentname = (isset($property['list_agent'])) ? $property['list_agent'] : '';
      $agentcode = (isset($property['agent_id'])) ? $property['agent_id'] : '';
      $agentphone = (isset($property['lagt_ph'])) ? $property['lagt_ph'] : '';
      $agent = [
        'name' => $agentname,
        'code' => $agentcode,
        'phone' => $agentphone,
      ];

      $coagentname = (isset($property["co_list"])) ? $property["co_list"] : '';
      $coagentcode = (isset($property["co_lagt_id"])) ? $property["co_lagt_id"] : '';
      $coagentphone = (isset($property["co_lagt_ph"])) ? $property["co_lagt_ph"] : '';
      $coagent = [
        'name' => $coagentname,
        'code' => $coagentcode,
        'phone' => $coagentphone,
      ];
      $agent_id = $this->get_agent_id($agent, $conection);
      $co_agent_id = $this->get_agent_id($coagent, $conection);

      $officename = (isset($property['rltr'])) ? $property['rltr'] : '';
      $officecode = (isset($property['code_treb'])) ? $property['code_treb'] : '';
      $officephone = (isset($property['bph_num'])) ? $property['bph_num'] : '';
      $office = [
        'name' => $officename,
        'code' => $officecode,
        'phone' => $officephone,
      ];

      $office_id = $this->get_office_id($office, $conection);
      $mls = $property['ml_num'];

      $sql = "Update idx_property_active_pending set agent_id=$agent_id,co_agent_id=$co_agent_id,
                                       office_id=$office_id where mls_num='$mls'";
      $conection->prepare($sql)->execute();

      IdxLogger::setLog("Property with mls_num $mls updated agent and offices");
    }
  }

  function insertToWorka($colum, $arrayDataProvider, $table, $class) {
    $colum[] = 'Class';
    $colum[] = 'MediaStatus';
    $colum[] = 'PhotosCount';

    foreach ($arrayDataProvider as $key => $value) {
      if ($class == "ResidentialProperty") {
        $mls = $value[23];
      }
      elseif ($class == "CondoProperty") {
        $mls = $value[40];
      }
      else {
        $mls = $value[25];
      }
      $sql = "Select MediaStatus,PhotosCount,MLS from Active_Property where MLS='$mls'";
      $photos = $this->connection->WorkaConnection()->query($sql)->fetch();
      $media = 0;
      $photosCount = 0;
      if (isset($photos['MediaStatus'])) {
        $media = $photos['MediaStatus'];
      }
      if (isset($photos['PhotosCount'])) {
        $photosCount = $photos['PhotosCount'];
      }

      $value[] = $class;
      $value[] = $media;
      $value[] = $photosCount;
      $arrayValues = [];
      foreach ($value as $item) {
        $arrayValues[] = empty($item) ? "null" : "'" . $this->string_sanitize($item) . "'";
      }
      $data = '(' . implode(",", $arrayValues) . ')';

      $this->registerDataInWorka(implode("`,`", $colum), $data, $table, $mls);
    }
  }

  function registerDataInWorka($fields, $arrayValues, $table, $mls) {
    if ($table == "Active_Property") {
      $sql = "REPLACE INTO $table(`$fields`) VALUES $arrayValues";
    }
    else {
      $sql = "INSERT IGNORE INTO $table($fields) VALUES $arrayValues";
    }

    $stmt = $this->connection->WorkaConnection()->prepare($sql);
    $stmt->execute();

    IdxLogger::setLog("Inserted Property with MLS: $mls");
  }

  public
  function string_sanitize($value
  ) {
    if (preg_match('/http/i', $value)) {
      $result = addslashes($value);
    }
    else {
      $result = preg_replace("/[^a-zA-Z0-9,.: !@*\+-]+/", "", html_entity_decode($value, ENT_QUOTES));
    }
    return $result;
  }

  function UpdateValuesToRDS($allProperties) {
    $offices = $this->getAllOfficesToShort($this->connection->ClosedConnection());
    foreach ($allProperties as $value) {
      $office = (NULL !== $value['ListOfficeMlsId']) ? $value['ListOfficeMlsId'] : 0;
      $office_id = $this->binarySearchAgentOffices($office, $offices);

      $coOffice = (NULL !== $value['CoListOfficeMlsId']) ? $value['CoListOfficeMlsId'] : 0;
      $co_office_id = $this->binarySearchAgentOffices($coOffice, $offices);

      $sysid = $value['MLS'];

      $sql = "Update idx_property_sold_rented set office_id = $office_id,co_office_id = $co_office_id where sysid = '$sysid'";
      $this->connection->ClosedConnection()->prepare($sql)->execute();
      $this->logger->successLog("Properties with sysid: $sysid Updated");
    }
  }

  public
  function processDataToInsertInWorka($propertiesArrayComing, $status = "Active"
  ) {
    foreach ($propertiesArrayComing['value'] as $k => $value) {
      $arrayValues = [];
      foreach ($value as $key => $item) {
        if (is_array($item)) {
          $value[$key] = implode(',', $item);
        }
      }
      $this->upsert($this->connection->WorkaConnection(), $value, $status == "Active" ? 'Active_Property' : 'Closed_Property');
    }
  }

  function upsert(PDO $pdo, array $properties, string $tableName = 'ActiveProperty') {
    $columns = array_keys($properties);

    $placeholders = array_fill(0, count($columns), '?');
    $updateFields = array_map(fn($col) => "`$col` = VALUES(`$col`)", $columns);

    $sql = "INSERT INTO `$tableName` (`" . implode('`,`', $columns) . "`)
            VALUES (" . implode(',', $placeholders) . ")
            ON DUPLICATE KEY UPDATE " . implode(', ', $updateFields);

    $stmt = $pdo->prepare($sql);

    $sanitizedValues = array_map(function($value) {
      if ($value === '') {
        return NULL;
      }

      if (is_string($value) && in_array(strtolower($value), [
          'true',
          'false',
        ], TRUE)) {
        return strtolower($value) === 'true' ? 1 : 0;
      }

      if (is_bool($value)) {
        return $value ? 1 : 0;
      }
      if (is_string($value) && in_array(strtolower($value), [
          'yes',
          'no',
        ], TRUE)) {
        return strtolower($value) === 'yes' ? 1 : 0;
      }
      if (is_string($value) && preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/', $value)) {
        try {
          $dt = new DateTime($value);
          return $dt->format('Y-m-d H:i:s');
        }
        catch (Exception $e) {
          return NULL;
        }
      }
      return $value;
    }, array_values($properties));
    $stmt->execute($sanitizedValues);
    IdxLogger::setLog("Procces Property with ListingKey: " . $properties['ListingKey'], IdxLog::type_success);
  }

  public
  function implodeArrayByComma($arrayToImplode
  ) {
    return (!empty($arrayToImplode)) ? implode(',', $arrayToImplode) : "(NULL)";
  }

  public
  function unionGenerateFields($property, $field, $cnt
  ) {
    $result = [];
    for ($i = 1; $i <= $cnt; $i++) {
      $position = $field . $i;
      if (!empty($property[$position])) {
        $result[] = $property[$position];
      }
    }

    return implode(',', $result);
  }

  public
  function sumGenerateFields($property, $field, $cnt
  ) {
    $result = 0;
    for ($i = 1; $i <= $cnt; $i++) {
      $position = $field . $i;
      if (!empty($property[$position])) {
        $result += $property[$position];
      }
      else {
        $result += 0;
      }
    }

    return $result;
  }

  public
  function getTimestampFormat($timestampValue
  ) {
    return (!is_null($timestampValue)) ? "'" . date_format(date_create($timestampValue), 'Y-m-d H:i:s') . "'" : 'NULL';
  }

  public
  function InsertPropertiesToWorka($implodedProperties, $keys, $table
  ) {
    $values = implode('),(', $implodedProperties);

    if ($table == "Active_Property") {
      $sqlString = "REPLACE INTO $table
    ($keys)
                        VALUES($values)";
    }
    else {
      $sqlString = "INSERT IGNORE INTO $table
    ($keys)
                        VALUES($values)";
    }

    return $this->connection->WorkaConnection()->prepare($sqlString)->execute();
  }

  public
  function getAllPropertiesWorka($table, $lastUpdate = NULL, $id = NULL
  ) {
    $lastUpdateQuery = '';
    if (!is_null($lastUpdate)) {
      $lastUpdateQuery = "WHERE(ModificationTimestamp > '$lastUpdate') and GeocodeStatus=1  order by id limit $id,1000";
    }
    else {
      if ($id >= 0) {
        $lastUpdateQuery = " order by id  limit $id,1000";
      }
    }

    $sqlString = "SELECT * FROM  $table $lastUpdateQuery ";
    var_dump($sqlString);
    $data = $this->connection->WorkaConnection()->query($sqlString)->fetchAll();

    return $data;
  }

  public
  function existInBD($sysid, $table, $con
  ) {
    $sql = "SELECT sysid FROM $table WHERE sysid = '$sysid'";
    $result = $con->query($sql)->fetch();
    return isset($result['sysid']);
  }

  public
  function existInBDGeocode($sysid, $table, $con
  ) {
    $sql = "SELECT lat FROM $table WHERE sysid = '$sysid'";
    $result = $con->query($sql)->fetch();
    return isset($result['lat']) ? TRUE : FALSE;
  }

  public
  function getpropertydownload($sysid, $table, $con
  ) {
    $sql = "Select property_downloaded,validate_image,date_create,img_cnt from $table where sysid = '$sysid'";
    $result = $con->query($sql)->fetch();

    return $result;
  }

  public
  function set_more_info($property
  ) {
    return [
      'type_property' => $property['PropertySubType'],
      'status_name' => $property['StandardStatus'],
      'style' => $property['ArchitecturalStyle'],
      'heating' => ($property['HeatType']),
      'waterfront_frontage' => $property['Waterfront'],
      'lot_size' => (int) $property['LotType'],
      'zoning' => $property['Zoning'],
      'county' => $property['CityRegion'],
      'parking_features' => ($property['ParkingFeatures']),
      'parking_total' => ($property['ParkingTotal']),
      'parking_spaces' => ($property['ParkingSpaces']),
      'sewer' => $property['Sewer'],
      'city' => $property['City'],
      'garage' => $property['GarageParkingSpaces'],
      'exterior_features' => ($property['ExteriorFeatures']),
      'water_source' => $property['Water'],
      'heat' => $property['HeatType'],
      'addres' => $property['UnparsedAddress'],
      'basement' => $property['Basement'],
      'pool_features' => $property['PoolFeatures'],
      'garage_type' => $property['GarageType'],
      'cooling' => $property['Cooling'],
      'security_features' => $property['SecurityFeatures'],
      'balcony' => $property['BalconyType'],
      'pets' => $property['PetsAllowed'],
      'postal_code' => $property['PostalCode'],
      'fireplace_features' => $property['FireplaceFeatures'],
      'features' => $property['PropertyFeatures'],
      'bath_total' => $property['BathroomsTotalInteger'],
      'living_area_range' => $property['LivingAreaRange'],
      'state' => $property['StateOrProvince'] ?? 'ON',
    ];
  }

  public
  function getClassId($property
  ) {
    $subtype = $property['PropertySubType'];
    $bussines = $property['BusinessType'];

    if (isset($this->globalVariables->class_id[$subtype])) {
      $class = $this->globalVariables->class_id[$subtype];
    }
    else {
      $class = (empty($property['UnitNumber'])) ? 2 : 1;
    }

    return $class;
  }

  public
  function get_city_id($value, $pdo
  ) {
    if (empty($value)) {
      return 1;
    }
    $city = trim(rtrim($value));
    $slug = filter_var($city, FILTER_SANITIZE_STRING);
    $slug = strtolower(preg_replace('/[^a-z0-9-]+/i', '-', $slug));

    $sql = "SELECT id FROM idx_city where code = '$slug'";
    $city_id = $pdo->query($sql)->fetch();
    if ($city_id) {
      return $city_id['id'];
    }
    else {
      $date = date('Y-m-d H:i:s');
      $query_insert = "INSERT INTO idx_city(name, code, create_at) VALUES('$city', '$slug', '$date') ";
      $pdo->prepare($query_insert)->execute();

      $sql = "SELECT id FROM idx_city where code = '$slug'";
      $city_id = $pdo->query($sql)->fetch();
      return $city_id['id'];
    }
  }

  public
  function binarySearchAgentOffices($value, $array
  ) {
    if ($value === NULL) {
      return 0;
    }

    foreach ($array as $item) {
      if ($item['code'] == $value) {
        return $item['id'];
      }
    }
    var_dump("entro");
    #Busqueda fallida
    return 0;
  }

  public
  function get_county_id($county, $pdo
  ) {
    if (empty($county)) {
      return 0;
    }
    $code = str_replace([' ', "'"], [' - ', ' - '], $county);
    $code = trim(rtrim($code));
    $code = strtolower($code);

    $county = addslashes($county);

    $sql = "SELECT id FROM idx_county where code = '" . $code . "'";
    $county_id = $pdo->query($sql)->fetch();
    if (isset($county_id['id'])) {
      return $county_id['id'];
    }
    else {
      $date = date('Y-m-d H:i:s');

      $query_insert = "INSERT INTO idx_county ( name,code,create_at) VALUES ('$county','$code','$date')";
      $pdo->prepare($query_insert)->execute();

      $sql = "SELECT id FROM idx_county where code = '$code'";
      $agent_id = $pdo->query($sql)->fetch();
      return $agent_id['id'];
    }
    return 0;
  }

  public
  function get_agent_id($agent, $con
  ) {
    if (empty($agent['code'])) {
      return 0;
    }
    //$code = str_replace('MFR', '', $agent);
    $sql = "SELECT id FROM idx_agent where code = '" . $agent['code'] . "'";
    $agent_id = $con->query($sql)->fetch();
    if (isset($agent_id['id'])) {
      return $agent_id['id'];
    }
    else {
      $date = date('Y-m-d H:i:s');
      $mui = 0;
      $query_insert = sprintf("insert into idx_agent (mui,code,name,phone,create_at) values ('%s','%s','%s','%s','%s')", $mui, $agent['code'], addslashes($agent['name']), $agent['phone'], $date);
      $con->prepare($query_insert)->execute();

      $sqloffice = "SELECT id FROM idx_agent where code = '" . $agent['code'] . "'";
      $office_id = $con->query($sqloffice)->fetch();
      return $office_id['id'];
    }
  }

  public
  function get_office_id($office, $con
  ) {
    if (empty($office['code'])) {
      return 0;
    }
    $sql = "SELECT id FROM idx_office where code = '" . $office['code'] . "'";
    $office_id = $con->query($sql)->fetch();
    if ($office_id) {
      return $office_id['id'];
    }
    else {
      $date = date('Y-m-d H:i:s');
      $mui = 0;
      $query_insert = sprintf("insert into idx_office (mui, code, name, create_at) values ('%s', '%s', '%s', '%s')", $mui, $office['code'], addslashes($office['name']), $date);
      $con->prepare($query_insert)->execute();

      $sqloffice = "SELECT id FROM idx_office where code = '" . $office['code'] . "'";
      $office_id = $con->query($sqloffice)->fetch();
      return $office_id['id'];
    }
  }

  public
  function UpdateProperties($lastUpdate
  ) {
    $offset = 0;
    do {
      $propertiesFromWorka = $this->getAllPropertiesWorka($this->workaTable, $lastUpdate, $offset);
      if (count($propertiesFromWorka) > 0) {
        IdxLogger::setLog("Saving properties from worka table ", IdxLog::type_confirmation);
        foreach ($propertiesFromWorka as $propertyKey => $propertyValue) {
          $params = $this->PrepareProperties($propertyValue);

          $this->InsertProperties($params);

          IdxLogger::setLog("Property with ListingId :" . $propertyValue['ListingKey'] . " Procesed", IdxLog::type_confirmation);
        }
      }
      $offset += count($propertiesFromWorka);
    }
    while (count($propertiesFromWorka) == 1000);

    if ($this->status == 'active') {
      $this->cleanIndex();
    }
  }

  public
  function cleanIndex() {
    $data = $this->getAllIdsToDelete();

    $this->deleteProperties($data);
  }

  public
  function getAllIdsToDelete() {
    $json = '{
                  "size":200000,
                  "_source": {
                    "includes": [
                      "mls_num",
                      "is_rental"
                    ]
                  }
                 }';
    $params = [
      'index' => $this->index . '_*',
      'body' => $json,
    ];

    $results = $this->client->search($params);
    $results = $results['hits']['hits'];
    $data = array_column($results, '_source');

    $sql = "Select ListingKey from Active_Property";
    $dataWorka = $this->connection->WorkaConnection()->query($sql)->fetchAll();
    $dataWorka = array_column($dataWorka, 'ListingKey');
    $result = [];
    foreach ($data as $value) {
      if (!in_array($value['mls_num'], $dataWorka)) {
        $result[] = $value['mls_num'];
      }
    }

    return ['ids' => $result, 'data' => array_column($results, '_source')];
  }

  public
  function deleteProperties($data
  ) {
    $idList = $data['ids'];
    $allData = $data['data'];

    $propertiesToDelete = [];

    foreach ($allData as $item) {
      if (in_array($item['mls_num'], $idList)) {
        $propertiesToDelete[] = $item;
      }
    }
    IdxLogger::setLog("Quantity of propertys to delete is " . count($propertiesToDelete), IdxLog::type_confirmation);
    foreach ($propertiesToDelete as $value) {
      if ($value['is_rental'] == 1) {
        $index = $this->index . '_rental';
      }
      else {
        $index = $this->index . '_sale';
      }
      $params = [
        'index' => "$index",
        'id' => "{$value['mls_num']}",
      ];

      $this->client->delete($params);

      IdxLogger::setLog("Property with mls {$value['mls_num']} deleted", IdxLog::type_confirmation);
    }
  }

  public
  function getLastUpdate() {
    $json = '{
                    "size": 1,
                    "sort": [
                     { "last_updated" : {"order" : "desc", "format": "strict_date_optional_time_nanos"}},
                        {
                            "_score": {
                                "order": "desc"
                            }
                        }
                    ],
                    "stored_fields": [
                        "last_updated"
                    ]
                }';

    $params = [
      'index' => "$this->index" . '_*',
      'body' => $json,
    ];
    $results = $this->client->search($params);

    $date = $results['hits']['hits'][0]['sort'][0];
    $result = date("c", strtotime("-2 hour", strtotime($date)));
    $result = str_replace('+00:00', '.000Z', $result);

    //        return '2024-06-13T18:52:24.000Z';
    return $result;
  }

  public
  function InsertProperties($params
  ) {
    if ($this->table == 'idx_property_active_pending') {
      $index = $this->index . '_' . $this->type;
      $this->create_index($index);

      $data = [
        'index' => $index,
        'id' => $params['mls_num'],
        'body' => json_encode($params, TRUE),
      ];
      $this->client->index($data);
      $this->logger->confirmationLog("Properties wit mls_num {$params['mls_num']}  Insert");
    }
    else {
      $date = date('Y');
      if ($this->year <= $date and $this->year != NULL) {
        $index = $this->index . '_' . $this->year;
        $this->create_index($index);

        $data = [
          'index' => $index,
          'id' => $params['mls_num'],
          'body' => json_encode($params, TRUE),
        ];
        $this->client->index($data);
        $this->logger->confirmationLog("Properties wit mls_num {$params['mls_num']}  Insert");
      }
      else {
        $this->logger->confirmationLog("Properties wit mls_num {$params['mls_num']}  not have correct year");
      }
    }
  }

  public
  function create_index($index
  ) {
    $data = [
      'index' => $index,
    ];

    if (!$this->client->indices()->exists($data)->asBool()) {
      $param = [
        'settings' => [
          'number_of_shards' => $this->shards,
          'number_of_replicas' => $this->replicas,
          'max_result_window' => 10000,
        ],
        'mappings' => [
          'properties' => [
            'address_large' => [
              'type' => 'text',
              'fields' => [
                'keyword' => ['type' => 'keyword'],
              ],
            ],
            'address_short' => [
              'type' => 'text',
              'fields' => [
                'keyword' => ['type' => 'keyword'],
              ],
            ],
            'adom' => ['type' => 'long'],
            'agent_id' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'email' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'phone' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'agent_seller_id' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'email' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'phone' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'amenities' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'area' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'assoc_fee' => ['type' => 'long'],
            'availability_date' => ['type' => 'date'],
            'bath' => ['type' => 'integer'],
            'price_sold' => ['type' => 'long'],
            'baths_half' => ['type' => 'integer'],
            'bed' => ['type' => 'integer'],
            'board_id' => ['type' => 'long'],
            'boat_dock' => ['type' => 'boolean'],
            'building_sqft' => ['type' => 'long'],
            'buyer_compensation' => ['type' => 'text'],
            'c_adon' => ['type' => 'long'],
            'check_price_change_timestamp' => [
              'type' => 'date',
              'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
            ],
            'city' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'city_code' => ['type' => 'text'],
            'city_id' => ['type' => 'integer'],
            'city_name' => ['type' => 'text'],
            'class_id' => ['type' => 'integer'],
            'client_id' => ['type' => 'long'],
            'co_agent_id' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'email' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'phone' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'co_agent_seller_id' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'email' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'phone' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'co_office_id' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'phone' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'co_office_seller_id' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'phone' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'complex' => [
              'type' => 'text',
              'fields' => [
                'keyword' => ['type' => 'keyword'],
              ],
            ],
            'condo_floor' => ['type' => 'long'],
            'condo_hotel' => ['type' => 'boolean'],
            'county' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'date_close' => ['type' => 'date_nanos'],
            'date_create' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'date_pending' => [
              'type' => 'date',
              'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
            ],
            'date_proccess' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'date_property' => [
              'type' => 'date',
              'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
            ],
            'date_update' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'development' => [
              'type' => 'text',
              'fields' => [
                'keyword' => ['type' => 'keyword'],
              ],
            ],
            'equestrian' => ['type' => 'boolean'],
            'feature_exterior' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'feature_interior' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'floor' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'folio_number' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'foreclosure' => ['type' => 'boolean'],
            'full_address' => [
              'type' => 'text',
              'fields' => [
                'keyword' => ['type' => 'keyword'],
              ],
            ],
            'furnished' => ['type' => 'boolean'],
            'gated_community' => ['type' => 'boolean'],
            'golf' => ['type' => 'boolean'],
            'guest_house' => ['type' => 'boolean'],
            'has_webp' => ['type' => 'long'],
            'hopa' => ['type' => 'boolean'],
            'image' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'imagens' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'img_cnt' => ['type' => 'integer'],
            'img_date' => [
              'type' => 'date',
              'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
            ],
            'is_commercial' => ['type' => 'boolean'],
            'is_occupied' => ['type' => 'boolean'],
            'is_rental' => ['type' => 'long'],
            'is_vacant' => ['type' => 'boolean'],
            'last_updated' => [
              'type' => 'date',
              'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
            ],
            'lat' => ['type' => 'float'],
            'legal_desc' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'list_date' => ['type' => 'integer'],
            'lng' => ['type' => 'float'],
            'location' => ['type' => 'geo_point'],
            'lot_desc' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'lot_size' => ['type' => 'integer'],
            'mf' => ['type' => 'boolean'],
            'mls_num' => ['type' => 'keyword'],
            'mls_status' => ['type' => 'long'],
            'more_info' => [
              'properties' => [
                'addres' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'appliance' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'architectural_style' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'assoc_fee_paid' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'association_includes' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'attached_garage' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'city' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'community_features' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'construction' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'cooling' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'county' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'covered_spaces' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'days_market' => ['type' => 'long'],
                'faces' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'floor_desc' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'frontage_lenght' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'garage' => ['type' => 'long'],
                'heating' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'levels' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'listing_type' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'lot_features' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'lot_size' => ['type' => 'long'],
                'ocupant_type' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'parking_features' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'patio_features' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'pets' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'pool_features' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'possession' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'postal_city' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'public_section' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'public_survey_township' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'roof' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'senior_community' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'sewer' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'state' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'status_name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'stories' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'style' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'tax_information' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'terms' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'type_property' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'unit' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'unit_building' => ['type' => 'long'],
                'view' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'water_source' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'waterfront_frontage' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'window_features' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'year_built_details' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'zoning' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'ocean_front' => ['type' => 'boolean'],
            'office_id' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'phone' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'office_seller_id' => [
              'properties' => [
                'code' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'name' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
                'phone' => [
                  'type' => 'text',
                  'fields' => [
                    'keyword' => [
                      'type' => 'keyword',
                      'ignore_above' => 256,
                    ],
                  ],
                ],
              ],
            ],
            'oh' => ['type' => 'integer'],
            'oh_info' => [
              'properties' => [
                'date' => [
                  'type' => 'date',
                  'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
                ],
                'end_time' => [
                  'type' => 'date',
                  'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
                ],
                'id' => ['type' => 'integer'],
                'start_time' => [
                  'type' => 'date',
                  'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
                ],
                'status' => ['type' => 'text'],
                'type' => ['type' => 'text'],
              ],
            ],
            'parking' => ['type' => 'integer'],
            'parking_desc' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'pending_date' => ['type' => 'integer'],
            'penthouse' => ['type' => 'boolean'],
            'pets' => ['type' => 'boolean'],
            'pool' => ['type' => 'boolean'],
            'price' => ['type' => 'long'],
            'price_origin' => ['type' => 'long'],
            'price_sqft' => ['type' => 'double'],
            'pt_view' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'reduced_price' => ['type' => 'float'],
            'remark' => ['type' => 'text'],
            'rg_id' => ['type' => 'long'],
            'short_sale' => ['type' => 'boolean'],
            'slug' => ['type' => 'text'],
            'sqft' => ['type' => 'integer'],
            'st_name' => [
              'type' => 'text',
              'fields' => [
                'keyword' => ['type' => 'keyword'],
              ],
            ],
            'st_number' => ['type' => 'keyword'],
            'state_name' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'status' => ['type' => 'integer'],
            'status_change' => [
              'type' => 'date',
              'format' => 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis',
            ],
            'status_name' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'style' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'subdivision' => [
              'type' => 'text',
              'fields' => [
                'keyword' => ['type' => 'keyword'],
              ],
            ],
            'sysid' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'tax_amount' => ['type' => 'long'],
            'tax_information' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'tax_year' => ['type' => 'long'],
            'tennis' => ['type' => 'boolean'],
            'thumbnail' => ['type' => 'keyword'],
            'thumbnail_url' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'tw' => ['type' => 'boolean'],
            'type' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'type_name' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'unit' => ['type' => 'keyword'],
            'unit_floor' => ['type' => 'long'],
            'unit_number' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'virtual_tour' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'wa' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
            'water_front' => ['type' => 'boolean'],
            'water_view' => ['type' => 'boolean'],
            'wv' => ['type' => 'text'],
            'year' => ['type' => 'integer'],
            'year_built' => ['type' => 'long'],
            'zip' => [
              'type' => 'text',
              'fields' => [
                'keyword' => [
                  'type' => 'keyword',
                  'ignore_above' => 256,
                ],
              ],
            ],
          ],
        ],
      ];

      $params = [
        'index' => $index,
        'body' => $param,
      ];
      $this->client->indices()->create($params);
      IdxLogger::setLog("Create index $this->index", IdxLog::type_confirmation);
    }
  }

  public
  function PrepareProperties($property
  ) {
    $params = [];

    //variables convertidas
    $more_info = $this->set_more_info($property);

    $class_id = $this->getClassId($property);

    $city_name = trim(rtrim($property['City']));
    $city = trim(rtrim($city_name));
    $slug = filter_var($city, FILTER_SANITIZE_STRING);
    $code = strtolower(preg_replace('/[^a-z0-9-]+/i', '-', $slug));
    $city = ['name' => $city, 'code' => $code];
    $officecode = (isset($property['MainOfficeKey'])) ? $property['MainOfficeKey'] : '';
    $officename = (isset($property['ListOfficeName'])) ? $property['ListOfficeName'] : '';
    $officePhone = (isset($property['ListOfficePhone'])) ? $property['ListOfficePhone'] : 0;
    $office = [
      'name' => $officename,
      'code' => $officecode,
      'phone' => $officePhone,
    ];

    $coofficecode = (isset($property['CoListOfficeKey'])) ? $property['CoListOfficeKey'] : '';
    $coofficename = (isset($property['CoListOfficeName'])) ? $property['CoListOfficeName'] : '';
    $coofficePhone = (isset($property['CoListOfficePhone'])) ? $property['CoListOfficePhone'] : 0;
    $cooffice = [
      'name' => $coofficename,
      'code' => $coofficecode,
      'phone' => $coofficePhone,
    ];

    $agentcode = (isset($property['ListAgentKey'])) ? $property['ListAgentKey'] : '';
    $agentname = (isset($property['ListAgentFullName'])) ? explode(',', $property['ListAgentFullName'])[0] : '';
    $agentphone = (isset($property['ListAgentDirectPhone'])) ? $property['ListAgentDirectPhone'] : '';
    $agentemail = (isset($property['ListAgentEmail'])) ? $property['ListAgentEmail'] : '';
    $agent = [
      'name' => $agentname,
      'code' => $agentcode,
      'phone' => $agentphone,
      'email' => $agentemail,
    ];

    $coagentcode = (isset($property['CoListAgentKey'])) ? $property['CoListAgentKey'] : '';
    $coagentname = (isset($property['CoListAgentFullName'])) ? explode(',', $property['CoListAgentFullName'])[0] : '';
    $coagentphone = (isset($property['CoListAgentDirectPhone'])) ? $property['CoListAgentDirectPhone'] : '';
    $coagentemail = (isset($property['CoListAgentEmail'])) ? $property['CoListAgentEmail'] : '';
    $coagent = [
      'name' => $coagentname,
      'code' => $coagentcode,
      'phone' => $coagentphone,
      'email' => $coagentemail,
    ];

    $county = trim(rtrim($property['CityRegion']));
    $county = trim(rtrim($county));
    $slug = filter_var($county, FILTER_SANITIZE_STRING);
    $code = strtolower(preg_replace('/[^a-z0-9-]+/i', '-', $slug));
    $country = ['name' => $county, 'code' => $code];

    $data = $this->formatAddress($property);
    $address_short = trim("{$property['StreetNumber']} {$data['streetAll']}");
    $st_name = $data['streetName'];

    $address_short = (isset($property['UnitNumber']) && $class_id == 1) ? $address_short . ' #' . addslashes(trim(str_replace('#', '', $property['UnitNumber']))) : $address_short;
    $address_large = '';
    if (isset($property['PostalCode']) && isset($property['City'])) {
      $address_large = (trim(preg_replace('!\s+!', ' ', implode(', ', [
        trim($property['City']),
        implode(' ', [
          $this->globalVariables->states[$property['StateOrProvince']] ?? 'ON',
          trim($property['PostalCode']),
        ]),
      ]))));
    }

    $fullAddress = $address_short . ', ' . $address_large;
    $fullAddress = trim($fullAddress);
    $price_origin = (isset($property['OriginalListPrice'])) ? (int) $property['OriginalListPrice'] : $property['ListPrice'];

    if ($property['ListPriceUnit'] == 'Sq M Net') {
      $price = $property['ListPrice'] * $property['BuildingAreaTotal'];
      $sqft = $property['BuildingAreaTotal'] * 10.7639;
      $price_sqft = !empty($sqft) ? $price / $sqft : 0;
    }
    elseif ($property['ListPriceUnit'] == 'Per Acre') {
      $price = $property['ListPrice'] * $property['BuildingAreaTotal'];
      $sqft = $property['BuildingAreaTotal'] * 43560;
      $price_sqft = !empty($sqft) ? $property['ListPrice'] / $sqft : 0;
    }
    elseif (preg_match('/Sq Ft/i', $property['ListPriceUnit']) == 1) {
      $price = $property['ListPrice'] * $property['BuildingAreaTotal'];
      $price_sqft = $property['ListPrice'] ?? 0;
      $sqft = $property['BuildingAreaTotal'];
    }
    else {
      $price = $property['ListPrice'];
      $price_sqft = (int) $property['BuildingAreaTotal'] != 0 ? $property['ListPrice'] / $property['BuildingAreaTotal'] : 0;
      $sqft = $property['BuildingAreaTotal'];
    }

    if (empty($sqft)) {
      $price = $property['ListPrice'];
      $sqftExploded = explode('-', $property['LivingAreaRange']);
      if (count($sqftExploded) == 2) {
        $sqft = $sqftExploded[1];
      }
      else {
        $sqft = (int) trim(str_replace(['+', '<', '>'], [
          '',
          '',
          '',
        ], $property['LivingAreaRange']));
      }
      $price_sqft = !empty($sqft) ? $price / $sqft : 0;
    }

    $lotSize = (!empty($property['LotWidth']) && !empty($property['LotDepth'])) ? $property['LotWidth'] * $property['LotDepth'] : 0;
    $params['sysid'] = $property['ListingKey'];
    $params['mls_num'] = $property['ListingKey']; //$property['ListingId'];
    $params['date_property'] = date('Y-m-d H:i:s', strtotime($property['OriginalEntryTimestamp']));

    $params['list_date'] = (!empty($property['OriginalEntryTimestamp'])) ? strtotime($property['OriginalEntryTimestamp']) : strtotime($property['ModificationTimestamp']);

    $params['class_id'] = $class_id;
    $params['city'] = $city;
    $params['county'] = $country;
    $params['board_id'] = 36;
    $params['city_name'] = $city_name;

    $params['office_id'] = $office;
    $params['co_office_id'] = $cooffice;

    $params['agent_id'] = $agent;
    $params['co_agent_id'] = $coagent;

    $params['address_short'] = str_replace("'", "\'", $address_short);
    $params['address_large'] = str_replace("'", "\'", $address_large);
    $params['full_address'] = $fullAddress;

    $params['price_origin'] = (int) $price_origin;
    $params['price'] = (int) $price;   //revisar en detalle si el close price es el current de los rental
    $params['is_rental'] = ($property['TransactionType'] == 'For Sale' || empty($property['TransactionType'])) ? 0 : 1;
    $params['year_built'] = (!empty($property['ApproximateAge'])) ? (int) $property['ApproximateAge'] : NULL;
    $params['type_name'] = $property['PropertySubType'];
    $params['type'] = $property['PropertyType'];
    $params['bed'] = (isset($property['BedroomsTotal'])) ? (($property['BedroomsTotal'] == NULL) ? 0 : (int) $property['BedroomsTotal']) : 0;
    $params['neighborhood'] = $property['CityRegion'];
    $params['baths_half'] = NULL;

    //    foreach ($this->globalVariables->baths as $key => $value) {
    //      if (!empty($property[$key])) {
    //        if ($property[$value] <= 2) {
    //          $params['baths_half']++;
    //        }
    //      }
    //    }
    $params['bath'] = $property['BathroomsTotalInteger'];

    $params['img_cnt'] = 0;

    $params['st_number'] = (isset($property['StreetNumber'])) ? $property['StreetNumber'] : NULL;
    $params['st_name'] = str_replace("'", "\'", $st_name);                                            //aumentar tamaño del campo de texto
    $params['unit'] = (!empty($property['UnitNumber']) && $params['class_id'] == 1) ? trim($property['UnitNumber']) : NULL;
    $params['img_date'] = (isset($property['PhotosChangeTimestamp']) && !empty($property['PhotosChangeTimestamp'])) ? date('Y-m-d H:i:s', strtotime($property['PhotosChangeTimestamp'])) : date('Y-m-d H:i:s', strtotime($property['OriginalEntryTimestamp']));
    $params['image'] = $params['mls_num'] . "_1.jpeg";
    if ($params['img_cnt'] > 0) {
      $params['image'] = $params['mls_num'] . "_1.jpeg";
    }
    $params['zip'] = $property['PostalCode'] ?? 0;

    $params['building_sqft'] = (int) $sqft ?? 0;;  //revisar en detalle
    $params['sqft'] = (int) $sqft ?? 0;
    $params['lot_size'] = $lotSize ?? 0;

    $params['lot_desc'] = (isset($property['LotFeatures'])) ? $property['LotFeatures'] : NULL;                  //aumentar tamaño de campo
    $params['legal_desc'] = (isset($property['LegalDescription'])) ? trim(preg_replace('!\s+!', ' ', $property['LegalDescription'])) : NULL;
    $params['amenities'] = $property['PropertyFeatures'] ?? NULL;
    if ($params['class_id'] == 1) {
      $params['amenities'] = $property['AssociationFeeIncludes'] ?? NULL;
    }

    $params['parking_desc'] = (isset($property['ParkingFeatures'])) ? addslashes($property['ParkingFeatures']) : NULL;

    $params['wv'] = $property['View'] ?? NULL;
    $params['water_front'] = !empty($property['WaterfrontYN']) ? TRUE : FALSE;

    $params['wa'] = (isset($property['Water'])) ? $property['Water'] : 'Public';

    $params['state'] = $property['StateOrProvince'] ?? 'ON';
    $params['state_name'] = $this->globalVariables->statesName[$property['StateOrProvince']] ?? 'ON';

    $params['parking'] = !empty($property['ParkingTotal']) ? $property['ParkingTotal'] : $property['ParkingSpaces'];

    $params['area'] = $property['RetailAreaCode'];

    $params['condo_floor'] = (isset($property['Level'])) ? (int) $property['Level'] : 0;
    $params['ocean_front'] = (isset($params['wv'])) ? ((preg_match('/Beach/i', $params['wv']) || preg_match('/Ocean/i', $params['wv'])) ? TRUE : FALSE) : FALSE;
    $params['water_view'] = (!empty($property['WaterView'])) ? TRUE : FALSE;
    $params['tw'] = preg_match('/Townhouse/i', $property['PropertySubType']) == 1 ? TRUE : FALSE;
    $params['floor'] = (isset($property['Basement1'])) ? $property['Basement1'] : '';
    $params['is_vacant'] = (preg_match('/Land/i', $property['PropertySubType']) == 1) ? TRUE : FALSE;;
    $params['furnished'] = ($property['Furnished'] == 'Furnished' || $property['Furnished'] == 'Partially') ? TRUE : FALSE;
    $params['foreclosure'] = (isset($property['PublicRemarks'])) ? (preg_match('/foreclosure/i', $property['PublicRemarks']) ? TRUE : FALSE) : FALSE;
    $params['penthouse'] = (isset($property['PublicRemarks'])) ? (preg_match('/penthouse/i', $property['PublicRemarks']) ? TRUE : FALSE) : FALSE;
    $params['pets'] = (preg_match('/Restricted/i', $property['PetsAllowed']) || $property['PetsAllowed'] == 1) ? TRUE : FALSE;
    $params['pool'] = !empty($property['PoolFeatures']) ? (preg_match('/None/i', $property['PoolFeatures']) == 1 ? FALSE : TRUE) : FALSE;

    $params['golf'] = (isset($property['PublicRemarks'])) ? (preg_match('/Golf/i', $property['PublicRemarks']) ? TRUE : FALSE) : FALSE;
    $params['tennis'] = (isset($property['PublicRemarks'])) ? (preg_match('/Tennis/i', $property['PublicRemarks']) ? TRUE : FALSE) : FALSE;
    $params['short_sale'] = (isset($property['PublicRemarks'])) ? (preg_match('/Short sale/i', $property['PublicRemarks']) ? TRUE : FALSE) : FALSE;
    $params['is_occupied'] = (isset($property['PublicRemarks'])) ? (preg_match('/Occupied/i', $property['PublicRemarks']) ? TRUE : FALSE) : FALSE;
    $params['guest_house'] = (isset($property['PublicRemarks'])) ? (preg_match('/Guest House/i', $property['PublicRemarks']) ? TRUE : FALSE) : FALSE;
    $params['gated_community'] = (isset($property['PublicRemarks'])) ? (preg_match('/Gaated Community/i', $property['PublicRemarks']) ? FALSE : TRUE) : FALSE;
    $params['equestrian'] = (isset($property['PublicRemarks'])) ? (preg_match('/Equestrian/i', $property['PublicRemarks']) ? TRUE : FALSE) : FALSE;
    $params['subdivision'] = $property['CityRegion'] ?? NULL;
    $params['boat_dock'] = (isset($property['equestrian'])) ? (preg_match('/Boat Dock/i', $property['equestrian']) ? TRUE : FALSE) : FALSE;
    $params['condo_hotel'] = (isset($property['equestrian'])) ? (preg_match('/Motel/i', $property['equestrian']) || preg_match('/Hotel/i', $property['equestrian']) ? TRUE : FALSE) : FALSE;

    $params['mf'] = ($params['class_id'] == 33) ? TRUE : FALSE;

    $params['oh'] = 0;

    $params['folio_number'] = (isset($property['ParcelNumber'])) ? $property['ParcelNumber'] : '';
    $params['style'] = (isset($property['ArchitecturalStyle'])) ? $property['ArchitecturalStyle'] : '';
    $params['date_create'] = date('Y-m-d H:i:s');
    $params['date_proccess'] = date('Y-m-d H:i:s');
    $params['last_updated'] = date('Y-m-d H:i:s', strtotime($property['ModificationTimestamp']));

    $params['mls_status'] = ('Active' === $property['StandardStatus']) ? 1 : 6;

    $params['status_name'] = $property['StandardStatus'];
    $params['slug'] = strtolower(preg_replace('!-+!', '-', preg_replace('/[^a-zA-Z0-9\-]/', '', str_replace(' ', '-', implode('-', [
      $params['address_short'],
      $params['address_large'],
      $params['mls_num'],
    ])))));

    $now = time(); // or your date as well
    $your_date = strtotime($property['OriginalEntryTimestamp']);
    $datediff = $now - $your_date;

    $params['adom'] = round($datediff / (60 * 60 * 24));;

    $params['tax_year'] = (isset($property['TaxYear'])) ? (int) $property['TaxYear'] : NULL;
    $params['tax_amount'] = (isset($property['TaxAnnualAmount'])) ? (int) $property['TaxAnnualAmount'] : NULL;
    $params['feature_exterior'] = (isset($property['ExteriorFeatures'])) ? $property['ExteriorFeatures'] : '';
    $params['virtual_tour'] = (isset($property['VirtualTourURL'])) ? $property['VirtualTourURL'] : NULL;
    $params['is_commercial'] = $property['PropertyType'] == 'Commercial' ? TRUE : FALSE;
    $params['more_info'] = $more_info;
    $previusListPrice = $property['PreviousListPrice'] ?? $price_origin;
    $params['reduced_price'] = ($previusListPrice > 0) ? ($price * 100) / $previusListPrice : 0;
    $params['reduced_price'] = ($params['reduced_price'] > 0) ? $params['reduced_price'] - 100 : 0;
    $params['reduced_price'] = round($params['reduced_price'], 2);

    $params['feature_interior'] = (isset($property['InteriorFeatures'])) ? $property['InteriorFeatures'] : NULL;
    $params['assoc_fee'] = (isset($property['AssociationFee'])) ? (int) $property['AssociationFee'] : 0;
    $params['price_sqft'] = $price_sqft;

    $params['pt_view'] = $property['View'] ?? NULL;

    $params['imagens'] = [];
    $params['development'] = $property['Development'] ?? NULL; //revisar OJO
    $params['complex'] = $property['Complex'] ?? NULL;  //revisar OJO
    $params['remark'] = $property['PublicRemarks'];
    $prefix = substr($params['mls_num'], -2);
    $params['img_cnt'] = $property['PhotosCount'];
    if ($params['status_name'] == 'Closed') {
      $params['imagens'] = $this->generatemediacollection(1, $params['mls_num']);
    }
    else {
      $params['imagens'] = $this->generatemediacollection($params['img_cnt'], $params['mls_num']);
    }

    if (!empty($property['Latitude']) && !empty($property['Longitude'])) {
      $params['location'] = "{$property['Latitude']},{$property['Longitude']}";
      $params['lat'] = isset($property['Latitude']) ? (float) $property['Latitude'] : NULL;
      $params['lng'] = isset($property['Longitude']) ? (float) $property['Longitude'] : NULL;
    }
    else {
      $params['location'] = NULL;
      $params['lat'] = NULL;
      $params['lng'] = NULL;
    }

    $params['image_url'] = 'https://' . $this->globalVariables->bucket_name . '.idxboost.us/' . $prefix . '/' . $params['mls_num'] . '_1.jpeg';
    $params['thumbnail_url'] = 'https://' . $this->globalVariables->bucket_name_reduced . '.idxboost.us/' . $prefix . '/' . $params['mls_num'] . '_x600.jpeg';;

    if ($this->table == "idx_property_active_pending") {
      $params['check_price_change_timestamp'] = $property['PriceChangeTimestamp'] ? date('Y-m-d H:i:s', strtotime($property['PriceChangeTimestamp'])) : date('Y-m-d H:i:s', strtotime($property['OriginalEntryTimestamp']));

      $this->type = ($params['is_rental'] == 1) ? 'rental' : 'sale';
    }
    else {
      $params['mls_status'] = (0 == $params['is_rental']) ? 2 : 5;
      $params['date_close'] = strtotime($property['CloseDate']);
      $params['price_sold'] = (int) $property['ClosePrice'];
      $new = $params['is_rental'] == 1 ? "rented-" : "sold-";
      $params['slug_sold'] = $new . strtolower(preg_replace('!-+!', '-', preg_replace('/[^a-zA-Z0-9\-]/', '', str_replace(' ', '-', implode('-', [
          $params['address_short'],
          $params['address_large'],
          $params['mls_num'],
        ])))));

      $params['living_size_m2'] = (int) $params['sqft'] / 10.764;
      $params['price_sqft_m2'] = $params['living_size_m2'] > 0 ? $params['price_sold'] / $params['living_size_m2'] : 0;
      $params['heading'] = $params['class_id'] == 1 ? (empty($params['development']) ? $params['complex'] : $params['development']) : (empty($params['subdivision']) ? $params['complex'] : $params['subdivision']);
      if ($params['date_close'] == 0) {
        $this->year = NULL;
      }
      else {
        $this->year = date('Y', $params['date_close']);
      }
    }

    return $params;
  }

  function imageResize($mls, $rute, $prefix, $alto) {
    $imageConverted = $mls . '_x' . $alto . '.jpeg';
    $destinoRoute = "../uploads/" . $imageConverted;
    $this->redimensionarJPEG($rute, $destinoRoute, $alto, $alto);

    $this->uploadImageReduced($destinoRoute, $imageConverted, $prefix);
    unlink($destinoRoute);
  }

  public
  function uploadImageReduced($path_to_file, $filename, $prefix
  ) {
    // setup s3 client
    $s3 = new \Aws\S3\S3Client([
      'version' => 'latest',
      'region' => 'nyc3',
      'endpoint' => 'https://nyc3.digitaloceanspaces.com',
      'credentials' => [
        'key' => $this->globalVariables->do_spaces_access_key_reduced,
        'secret' => $this->globalVariables->do_spaces_secret_key_reduced,
      ],
    ]);

    // upload file
    $s3_request = $s3->putObject([
      'ContentLength' => (int) filesize($path_to_file),
      'ContentType' => 'image/jpeg',
      'Bucket' => $this->globalVariables->bucket_name_reduced,
      'Key' => implode('/', [$prefix, $filename]),
      'Body' => file_get_contents($path_to_file),
      'CacheControl' => 'max-age=31536000',
      'ACL' => 'public-read',
    ]);

    $s3_response = $s3_request->toArray();
    $object_url = $s3_response['ObjectURL'];
    return $filename;
  }

  function updateValidateImage($mls, $id, $pdoc) {
    $sql = "Update idx_property_active_pending set validate_image=1 where mls_num = '$mls'";
    $pdoc->ActiveConnection()->prepare($sql)->execute();

    IdxLogger::setLog("Download Images for Property  whith mls_num:  $mls  and id  $id ", IdxLog::type_success);
  }

  function redimensionarJPEG($origen, $destino, $ancho_max, $alto_max) {
    $image = new ImageResize($origen);
    $image->resizeToBestFit($ancho_max, $alto_max);
    $image->save($destino);
  }

  public
  function formatAddress($property
  ) {
    $stName = $property['StreetName'];

    $streetName = [];

    $stSufix = (empty($property['StreetSuffix']) || $property['StreetSuffix'] == 'N\A') ? $property['StreetSuffixCode'] : $property['StreetSuffix'];
    $suffix = isset($suffixArray[strtoupper($stSufix)]) ? ucwords(strtolower($this->globalVariables->suffixArray[strtoupper($stSufix)])) : $stSufix;

    $prefix = trim($property['StreetDirPrefix']);
    $prefix = isset($prefixArray[strtoupper($prefix)]) ? trim($this->globalVariables->prefixArray[strtoupper($prefix)]) : $prefix;

    $result['streetName'] = $stName;
    $result['streetName'] = trim(preg_replace('!\s+!', ' ', $result['streetName']));

    $result['streetAll'] = ($prefix . ' ' . ($result['streetName']) . ' ' . $suffix);
    $result['streetAll'] = trim(preg_replace('!\s+!', ' ', $result['streetAll']));
    return $result;
  }

  public
  function generateWaterFeatures($view
  ) {
    $result = [];

    $view = explode(',', $view);

    foreach ($this->globalVariables->wvFeatures as $key => $value) {
      if (in_array($value, $view)) {
        if (!in_array($this->globalVariables->newWvFeatures[$key], $result)) {
          $result[] = $this->globalVariables->newWvFeatures[$key];
        }
      }
    }

    $return = count($result) > 0 ? implode(',', $result) : '';

    return $return;
  }

  public function upserActivePending() {}

  //data for property extra
  public
  function InsertPropertyExtra($property, $con
  ) {
    $params = [];

    $params['sysid'] = $property['MLS'];

    $remark = (isset($property['RemarksForClients'])) ? (trim(preg_replace('!\s+!', ' ', $property['RemarksForClients']))) : '';
    $remark = str_replace("\\", "", $remark);
    $params['remark'] = str_replace("'", "", $remark);

    $feature_interior = (isset($property['InteriorFeatures'])) ? $property['InteriorFeatures'] : '';
    $params['feature_interior'] = str_replace("'", "", $feature_interior);

    $feature_exterior = (isset($property['Exterior1'])) ? $property['Exterior1'] : '';
    $params['feature_exterior'] = str_replace("'", "", $feature_exterior);

    $amenities = $this->unionGenerateFields($property, 'PropertyFeatures', 3);
    $params['amenities'] = $amenities;
    $params['assoc_fee'] = (isset($property['AssociationFeeFrecuency'])) ? (int) $property['AssociationFeeFrecuency'] : 0;
    $virtualtour = (isset($property['VirtualTour'])) ? $property['VirtualTour'] : NULL;
    $virtualtour = str_replace("'", "", $virtualtour);
    $params['virtual_tour'] = str_replace('\\', '/', $virtualtour);
    $params['area'] = $property['AreaCode'];
    $params['unit_number'] = (isset($property['UnitNumber'])) ? $property['UnitNumber'] : NULL;
    $params['date_update'] = date('Y-m-d H:i:s', strtotime($property['PixUpdtedDt']));
    $params['type'] = (isset($property['Style'])) ? ucwords(strtolower($property['Style'])) : '';
    $params['tax_amount'] = (isset($property['Taxes'])) ? ($property['Taxes']) : 0;
    $params['tax_year'] = (isset($property['TaxYear']) && $property['TaxYear'] != "Current") ? preg_replace('/([A-Z][a-z] )*/i', '', $property['TaxYear']) : 2022;

    $params['imagens'] = $this->generatemediacollection($property['PhotosCount'], $property['MLS']);

    $params['address_map'] = $property['Address'];
    $sql = '';
    if (!$this->existInBD($params['sysid'], 'idx_property_extra', $con)) {
      $sql = "
                INSERT INTO idx_property_extra(
                    sysid,
                    type,
                    unit_number,
                    remark,
                    feature_interior,
                    feature_exterior,
                    amenities,
                    assoc_fee,
                    virtual_tour,
                    area,
                    imagens,
                    address_map,
                    date_update,
                    tax_year,
                    tax_amount
            )
            VALUES(
                '" . $params['sysid'] . "',
                '" . $params['type'] . "',
                '" . $params['unit_number'] . "',
                '" . $params['remark'] . "',
                '" . $params['feature_interior'] . "',
                '" . $params['feature_exterior'] . "',
                '" . $params['amenities'] . "',
                " . $params['assoc_fee'] . ",
                '" . $params['virtual_tour'] . "',
                '" . $params['area'] . "',
                '" . $params['imagens'] . "',
                '" . $params['address_map'] . "',
                '" . $params['date_update'] . "',
                 '" . $params['tax_year'] . "',
                 '" . $params['tax_amount'] . "'
                 ) ";

      $stmt = $con->prepare($sql);
      $stmt->execute();
      IdxLogger::setLog("sysid: {$params['sysid']} Inserted in extra", IdxLog::type_success);
    }
    else {
      $sql = "update idx_property_extra set type = '" . $params['type'] . "', unit_number = '" . $params['unit_number'] . "', remark= '" . $params['remark'] . "',feature_interior='" . $params['feature_interior'] . "',feature_exterior='" . $params['feature_exterior'] . "',
                     amenities='" . $params['amenities'] . "',assoc_fee=" . $params['assoc_fee'] . ", virtual_tour='" . $params['virtual_tour'] . "', area='" . $params['area'] . "',address_map='" . $params['address_map'] . "',
                    date_update='" . $params['date_update'] . "' where sysid = '" . $params['sysid'] . "';";

      $stmt = $con->prepare($sql);
      $stmt->execute();
      IdxLogger::setLog("sysid: {$params['sysid']} Updated in extra", IdxLog::type_success);
    }
  }

  public
  function generatemediacollection($imgCount, $mlsNumber
  ) {
    $collection = [];
    $prefix = substr($mlsNumber, -2);
    for ($i = 1; $i <= $imgCount; $i++) {
      $collection[] = 'https://ib-36-photos.idxboost.us/' . $prefix . '/' . (string) $mlsNumber . '_' . $i . '.' . 'jpeg';
    }
    var_dump($collection);
    return $collection;
  }

  function getPriceChangeTimestamp($property, $con) {
    $mls = $property['MLS'];

    $sql = "SELECT price,check_price_change_timestamp FROM idx_property_active_pending  where mls_num = '$mls'";
    $data = $con->query($sql)->fetch();

    $result = NULL;
    if (empty($data)) {
      $result = date('Y-m-d H:i:s', strtotime($property['PixUpdtedDt']));

      if ($property['ListPrice'] == $property['ListPrice']) {
        $result = date('Y-m-d H:i:s', strtotime($property['TimestampSql']));
      }
    }
    else {
      if ($property['ListPrice'] != $data['price']) {
        $result = date('Y-m-d H:i:s', strtotime($property['PixUpdtedDt']));
      }
      else {
        $result = empty($result) ? date('Y-m-d H:i:s', strtotime($data['check_price_change_timestamp'])) : $result;
      }
    }

    return $result;
  }

  public
  function getPriceSqft($property
  ) {
    if (isset($property['LivingAreaSqft'])) {
      $result = !empty($property['LivingAreaSqft']) ? $property['ListPrice'] / $property['LivingAreaSqft'] : NULL;
    }
    else {
      $result = 0;
    }

    return $result;
  }

  public
  function getAllOfficesToShort($con
  ) {
    $sql = 'SELECT id, code FROM idx_office order by code asc ';
    $sqlToExec = $con->query($sql)->fetchAll();
    return $sqlToExec;
  }

  public
  function getAllAgentsToShort($con
  ) {
    $sql = 'SELECT id, code FROM idx_agent order by code asc ';
    $sqlToExec = $con->query($sql)->fetchAll();
    return $sqlToExec;
  }

  public
  function InsertPropertyGeocode($property, $con
  ) {
    $sql = "";
    $sysid = $property['MLS'];

    $test = $this->existInBDGeocode($sysid, 'idx_property_geocode', $con);
    if ($test == FALSE) {
      $sql = "INSERT Ignore INTO idx_property_geocode_backup(sysid) VALUES ('$sysid') ";
      $con->prepare($sql)->execute();

      IdxLogger::setLog("Sysid: $sysid Inserted in Geocode", IdxLog::type_success);
    }
    else {
      IdxLogger::setLog("Sysid: $sysid Exists in geocode", IdxLog::type_success);
    }
  }

  function insertLastUpdatePropertiesCpanel() {
    $date = gmdate('Y-m-d H:i:s');
    $date = strtotime('-4 Hours', strtotime($date));
    $date = gmdate("Y-m-d H:i:s", $date);

    $sql = "UPDATE flex_idx_boards SET last_check_timestamp = '$date' WHERE code = 17";

    $this->connection->connectionForPropertiesCpanel()
      ->prepare($sql)
      ->execute();
  }

  public
  function getLastUpdatePhoto($con, $table
  ) {
    $lastUpdate = NULL;
    $sql = "SELECT img_date FROM $table WHERE property_downloaded =0 ORDER BY img_date DESC LIMIT 1";
    $result = $con->query($sql)->fetchAll();

    if (isset($result[0]['img_date'])) {
      $lastUpdate = str_replace("+", "-", $result[0]['img_date']);
      $lastUpdate = strtotime('-2 hours', strtotime($lastUpdate));

      $lastUpdate = date("Y-m-d H:i:s", $lastUpdate) . "-00:00";

      $lastUpdate = str_replace(" ", 'T', $lastUpdate);

      $lastUpdate = str_replace("-05:", "+00:", $lastUpdate);
      $lastUpdate = str_replace("-00:00", "Z", $lastUpdate);
    }
    else {
      $currentDate = date('d-m-Y');
      $temp = strtotime('-2 hours', strtotime($currentDate));
      $currentDateFormat = date("Y-m-d", $temp) . "00:00:00";
      $final = DateTime::createFromFormat("Y-m-d H:i:s", $currentDateFormat);
      $lastUpdate = str_replace(" ", 'T', $final->format(DateTime::RFC3339));
      $lastUpdate = str_replace("-05:", "+00:", $lastUpdate);
      $lastUpdate = str_replace("+00:00", "Z", $lastUpdate);
      $lastUpdate = str_replace("+", "-", $lastUpdate);
    }

    return $lastUpdate;
  }

  public
  function getPropertyImageChanged($con, $table, $lastUpdate
  ) {
    $sql = "SELECT sysid,mls_num FROM $table WHERE (property_downloaded = 0 and img_cnt>0) limit 1000";
    //        $sql = "SELECT sysid,mls_num FROM $table WHERE mls_num='210510'";
    $result = $con->query($sql)->fetchAll();

    return $result;
  }

  public
  function getImageByMls($mls, $cookies
  ) {
    $rets = $cookies['rets'];
    $requestHeaders['Cookie'] = "RETS-Session-ID=$rets;";

    $getAllProperties = $this->client->request('GET', "{$this->globalVariables->endpointMetadata}/getObject?rets-version=rets/1.8&Type=Photo&Resource=Property&Location=1&Id=$mls:*", [
      'headers' => $requestHeaders,
      'auth' => [
        $this->globalVariables->user,
        $this->globalVariables->pass,
        'basic',
      ],
    ]);

    $response = $getAllProperties->getBody()->getContents();

    $arrayImages = preg_split('/--(([0-9]+)||([a-z]+)||(.)*)/i', $response);
    unset($arrayImages[0]);
    unset($arrayImages[count($arrayImages) - 1]);
    return $arrayImages;
  }

  function getImageFromAmazon($mls, $photosCnt) {
    $name = $mls . "_" . $photosCnt . ".jpeg";
    $prefix = substr($mls, -2);
    $curl = curl_init();
    $url = "https://ib-b17-photos.idxboost.us/$prefix/$name";
    curl_setopt_array($curl, [
      CURLOPT_URL => $url,
      CURLOPT_RETURNTRANSFER => TRUE,
      CURLOPT_ENCODING => '',
      CURLOPT_MAXREDIRS => 10,
      CURLOPT_TIMEOUT => 2,
      CURLOPT_FOLLOWLOCATION => TRUE,
      CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
      CURLOPT_CUSTOMREQUEST => 'GET',
    ]);

    $response = curl_exec($curl);
    $httpCode = curl_getinfo($curl, CURLINFO_HTTP_CODE);

    return ['code' => $httpCode, 'image' => $response];
  }

  public
  function allProcess($allProperties, $dowloadFolder, $cant, $con, $table, $cookies
  ) {
    foreach ($allProperties as $Key => $value) {
      $imageByMls = [];

      $images = $this->getImageByMls($value['sysid'], $cookies);
      $split = count($images);

      if ($table == 'idx_property_sold_rented') {
        if (count($images) > 1) {
          $split = 1;
        }
      }

      if (is_array($images)) {
        for ($i = 1; $i < $split; $i++) {
          $temp = ($dowloadFolder . "/" . $value['mls_num'] . "_" . $i . ".jpeg");

          $imageContent = substr($images[$i], strpos($images[$i], 'Location:') + 10, strlen($images[$i]));
          $url = explode("\r\n", $imageContent);
          $imageContent = $url[0];

          $imgcontent = "https:" . $imageContent;
          file_put_contents($temp, file_get_contents($imgcontent));

          $obj['path_to_file'] = $temp;
          $obj['filename'] = $value['mls_num'] . "_" . $i . ".jpeg";
          $obj['prefix'] = substr($value['mls_num'], -2);
          if ($i == 1) {
            $this->imageResize($value['mls_num'], $temp, $obj['prefix'], '600');
            $this->imageResize($value['mls_num'], $temp, $obj['prefix'], '300');
          }
          $date = date('c');
          IdxLogger::setLog("Download image " . $value['mls_num'] . "_" . $i . ".jpeg ", IdxLog::type_success);
          $imageByMls[] = $this->uploadImage($obj['path_to_file'], $obj['filename'], $obj['prefix']);
          unlink($obj['path_to_file']);

          $cant++;
        }
        $this->updateImagesPropertiesExtra(serialize($imageByMls), $value['sysid'], $con);
        unset($imageByMls);
        $this->updatePropertiesDownloadPhoto($value['sysid'], $con, $table);
        IdxLogger::setLog(" Images for Property: " . $value['mls_num'] . " uploaded " . $cant, IdxLog::type_success);
      }
      else {
        IdxLogger::setLog("Images for sysid " . $value['sysid'] . " registred", IdxLog::type_success);
      }
      sleep(1);
      unset($allProperties[$Key]);
    }
  }

  public
  function updateImagesPropertiesExtra($images, $sysid, $con
  ) {
    $sql = "UPDATE idx_property_extra SET imagens='$images' WHERE sysid = '$sysid' ";
    $con->prepare($sql)->execute();
  }

  public
  function updatePropertiesDownloadPhoto($sysid, $con, $table
  ) {
    $sql = "UPDATE $table SET property_downloaded =1,validate_image=1 WHERE sysid = '$sysid'";
    $con->prepare($sql)->execute();
  }

  public
  function uploadImage($path_to_file, $filename, $prefix
  ) {
    // setup s3 client
    $s3 = new \Aws\S3\S3Client([
      'version' => 'latest',
      'region' => 'nyc3',
      'endpoint' => 'https://nyc3.digitaloceanspaces.com',
      'credentials' => [
        'key' => $this->globalVariables->do_spaces_access_key,
        'secret' => $this->globalVariables->do_spaces_secret_key,
      ],
    ]);

    // upload file
    $s3_request = $s3->putObject([
      'ContentLength' => (int) filesize($path_to_file),
      'ContentType' => 'image/jpeg',
      'Bucket' => $this->globalVariables->bucket_name,
      'Key' => implode('/', [$prefix, $filename]),
      'Body' => file_get_contents($path_to_file),
      'CacheControl' => 'max-age=31536000',
      'ACL' => 'public-read',
    ]);

    $s3_response = $s3_request->toArray();
    $object_url = $s3_response['ObjectURL'];
    return $filename;
  }

  public
  function loadDataPropertyGeocode($originArray, $con
  ) {
    $day = date("Y-m-d");

    if (count($originArray) > 0) {
      foreach ($originArray as $property) {
        $sysid = $property['MLS'];

        $address = $property['AddressMap'];

        if (!empty($address)) {
          //                    $sql = "Select request from api_board_21 where date='$day'";
          //                    $test = $this->connection->connectionForCoordinatesControl()->query($sql)->fetchAll();
          //                    if (!isset($test['request'])) {
          //                        $sql = "Insert into api_board_21(request,date) values (0,'$day')";
          //                        $this->connection->connectionForCoordinatesControl()->prepare($sql)->execute();
          //
          //                        $sql = "Select request from api_board_21 where date='$day'";
          //                        $test = $this->connection->connectionForCoordinatesControl()->query($sql)->fetchAll();

          var_dump($address);
          $coord = $this->getCoordinatesByAddress($address);
          var_dump($coord);
          $lat = isset($coord['lat']) ? $coord['lat'] : NULL;
          $lng = isset($coord['lon']) ? $coord['lon'] : NULL;
          if (!empty($lat) && !empty($lng)) {
            $sql = "Replace into idx_property_geocode (sysid,lat,lng,location) values ($sysid,$lat,$lng,POINT($lat,$lng))";
            $con->prepare($sql)->execute();

            $sql = "Delete from idx_property_geocode_backup where sysid='$sysid'";
            $con->prepare($sql)->execute();
            $this->logger->successLog("sysid: $sysid Processed!");
          }
          else {
            $sql = "Update idx_property_geocode_backup set proccesed=1 where sysid='$sysid'";
            $con->prepare($sql)->execute();

            $this->logger->confirmationLog("sysid: $sysid Not Processed due to wrong address or coordinates:  $address");
          }
        }
      }
    }
  }

  function getCoordinatesByAddress($address, $board) {
    $api_key = $this->globalVariables->{'YOUR_API_KEY_' . $board};
    if (!empty($address)) {
      $replace = [" " => " + ", "#" => ""];
      $addressFormated = trim(str_replace(array_keys($replace), array_values($replace), $address));

      $uri = "https://maps.googleapis.com/maps/api/geocode/json?address=$addressFormated&key={$api_key}";
      $response = $this->clientGuzzle->request('GET', $uri, []);
      if ($response->getStatusCode() == 200) {
        $coordinates = json_decode($response->getBody()->getContents(), TRUE);
        $coordinates = ($coordinates['status'] == 'OK') ?
          $coordinates['results'][0]['geometry']['location'] :
          [];
      }
      else {
        $this->logger->confirmationLog("Unauthorized");
      }
    }

    return $coordinates;
  }

  public
  function getMapAddresses($table, $pdo, $top
  ) {
    //        $sql = "SELECT TRIM(SUBSTRING_INDEX(UnparsedAddress, '-', -1)) as address,ListingKey  FROM $table  where  Latitude is null and ListingKey in ('C12104017','E12178718','C12160037','W12160344')  order by id desc limit 3000 ";
    $sql = "SELECT TRIM(SUBSTRING_INDEX(UnparsedAddress, '-', -1)) as address,ListingKey  FROM $table  where  Latitude is null and GeocodeStatus=0 and UnparsedAddress is not null   order by id desc limit $top ";
    $data = $pdo->query($sql)->fetchAll();

    return $data;
  }

  public
  function existAddress($con, $address, $ListingKey, $table
  ) {
    $address = addslashes($address);
    $sql = "Select Latitude,Longitude from GeocodeBackup where address='$address'";
    $data = $con->query($sql)->fetch();
    if (isset($data['Latitude'])) {
      $lat = $data['Latitude'];
      $lng = $data['Longitude'];

      $sql = "Update $table set Latitude=$lat, Longitude=$lng, GeocodeStatus=1 where ListingKey='$ListingKey'";
      $con->prepare($sql)->execute();

      IdxLogger::setLog("Geocode are downnloaded for sysid $ListingKey", IdxLog::type_confirmation);
      return FALSE;
    }
    return TRUE;
  }

  public
  function loadDataPropertyGeocode2($originArray, $table, $con, $board
  ) {
    $day = date("Y-m-d");

    if (count($originArray) > 0) {
      foreach ($originArray as $property) {
        sleep(1);
        try {
          if ($this->existAddress($con, $property['address'], $property['ListingKey'], $table)) {
            $sysid = $property['ListingKey'];

            if (!empty($property['address'])) {
              $sql = "Select sum(requests) as requests from gmaps_api_control.api_board_{$board} where date='$day';";
              $test = $this->connection->ActiveConnection()
                ->query($sql)
                ->fetch();

              if (!isset($test['requests'])) {
                $sql = "Insert into gmaps_api_control.api_board_{$board}(requests,date) values (0,'$day')";
                $this->connection->ActiveConnection()->prepare($sql)->execute();

                $test['requests'] = 0;
              }

              if ($test['requests'] > 5000) {
                var_dump("Limite excedido de peticiones");
                die;
              }

              $coord = $this->getCoordinatesByAddress($property['address'], $board);

              $lat = $coord['lat'] ?? NULL;
              $lon = $coord['lng'] ?? NULL;

              if (!empty($lat) && !empty($lon)) {
                $sql = "Update $table set Latitude=$lat,Longitude=$lon,GeocodeStatus=1 where ListingKey='$sysid'";
                $con->prepare($sql)->execute();
                $property['address'] = addslashes($property['address']);
                /*Saving Property in All Geocode*/
                $sql2 = "Insert  into GeocodeBackup (address,Latitude,Longitude) values ('{$property['address']}','$lat','$lon')";
                $con->prepare($sql2)->execute();

                IdxLogger::setLog("Geocode proccesed for board [{$board}] whit sysid: " . $property['ListingKey']);
              }
              else {
                $sql = "Update $table set GeocodeStatus=3 where ListingKey='$sysid'";
                $con->prepare($sql)->execute();

                echo "sysid: " . $sysid . " Not Processed due to wrong address or coordinates..." . " \n ";
              }
              $sql = "Insert into gmaps_api_control.api_board_{$board}(requests,date) values (1,'$day')";
              $this->connection->ActiveConnection()->prepare($sql)->execute();
            }
          }
        }
        catch (Exception $e) {
          var_dump($e->getMessage());
        }
      }
    }
  }

  public
  function getMapAddressesForAgentId($table, $con, $id
  ) {
    $result = [];

    $sql = "SELECT sysid, address_short,address_large  FROM $table join idx_property_geocode using (sysid) where (lat IS NULL AND lng IS null) AND (agent_id=$id OR co_agent_id=$id OR agent_seller_id=$id OR co_agent_seller_id=$id) limit 2000";
    $data = $con->query($sql)->fetchAll();

    foreach ($data as $key => $value) {
      $result[$key] = [
        'MLS' => $value['sysid'],
        'AddressMap' => $value['address_short'] . ', ' . $value['address_large'],
      ];
    }

    return $result;
  }

  public
  function getAllWorkaIdsToRemove() {
    $sql = "Select MLS from Active_Property ";
    $result = $this->connection->WorkaConnection()->query($sql)->fetchAll();

    $data = [];
    foreach ($result as $value) {
      $data[] = $value['MLS'];
    }

    return $data;
  }

  public
  function deleteRdsNoCommingProperties($workaIds
  ) {
    $workaPropertyIds = implode("','", $workaIds);

    $workaPropertyIds = "('" . $workaPropertyIds . "')";

    $activeCon = $this->connection->ActiveConnection();

    $this->logger->successLog("Deleting properties from Active Pending");
    $deleteAP = "DELETE FROM idx_property_active_pending where sysid not in $workaPropertyIds";
    $activeCon->prepare($deleteAP)->execute();

    $this->logger->successLog("Deleting properties from Active Pending");
    $deleteAP = "DELETE FROM idx_property_extra where sysid not in $workaPropertyIds";
    $activeCon->prepare($deleteAP)->execute();

    $this->logger->successLog("Deleting properties from Active Pending");
    $deleteAP = "DELETE FROM idx_property_geocode where sysid not in $workaPropertyIds";
    $activeCon->prepare($deleteAP)->execute();
  }

  public
  function getPropertiesForUpdatePhotos($board, $status, $mediaType = NULL
  ) {
    $con = $this->connection->WorkaConnection();
    $table = ($status == 'Active') ? 'Active_Property' : 'Closed_Property';

    $sql = "Select ListingKey as ListingId,ListingKey as ListingKey from $table where (MediaStatus not in (1,2) or MediaStatus is null) order by ModificationTimestamp desc limit 1000";
    return $con->query($sql)->fetchAll();
  }

  public
  function updateMediaStatus($mls_list, $status, $mediaValue
  ) {
    $con = $this->connection->WorkaConnection();

    $table = ($status == 'Active') ? 'Active_Property' : 'Closed_Property';

    $sql = "Update  $table  set MediaStatus=$mediaValue where ListingKey in ($mls_list) ";
    $con->prepare($sql)->execute();
  }

  public
  function getImageByProvider($con, $sysid, $status
  ) {
    $token = $this->globalVariables->VOW_TOCKEN;

    $url = 'https://query.ampre.ca/odata/Media?$filter=ResourceRecordKey eq \'' . $sysid . '\' and ImageSizeDescription eq \'Largest\' and MediaStatus eq \'Active\'&$select=MediaURL,Order&$orderby=Order';

    $response = $this->clientGuzzle->request('GET', $url, [
      'headers' => [
        'Authorization' => $token,
        'Accept' => 'application/json',
      ],
    ]);
    $result = json_decode($response->getBody()->getContents(), TRUE);
    if (count($result) > 0) {
      $result = array_column($result['value'], 'MediaURL');
      $imgCnt = count($result);
    }
    else {
      $imgCnt = 0;
      $result = [];
    }
    $table = ($status == 'Active') ? 'Active_Property' : 'Closed_Property';
    $sql = "Update $table set PhotosCount=$imgCnt where ListingKey='{$sysid}'";
    $con->prepare($sql)->execute();
    return $result;
  }

  public
  function registerFailure(array $failureData
  ): void {
    $connection = $this->connection->ActiveConnection();
    [
      'mls_num' => $mlsNum,
      'sysid' => $sysid,
      'message' => $message,
      'process' => $process,
      'boardId' => $boardId,
    ] = $failureData;

    $sql = "REPLACE INTO idxboost_datamonitor_setting_db.process_failures (board_id,mls_num,sysid,message,process) VALUES ('{$boardId}','{$mlsNum}','{$sysid}','{$message}','{$process}');";
    $connection->prepare($sql)->execute();
  }

  function updatePropertiesDownloadPhotoFailed($sysid, $table, $con) {
    $sql = "UPDATE  $table SET property_downloaded = 0 WHERE sysid = {$sysid}";
    $con->prepare($sql)->execute();
  }

}