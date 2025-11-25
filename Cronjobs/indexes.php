<?php
require_once '../autoload.php';

$helpers = new Helpers('closed');

$indexes=array('index_closed_42_2022','index_closed_42_2021','index_closed_42_2020','index_closed_42_2019','index_closed_42_2023','index_closed_42_2024','index_closed_42_2025');

foreach ($indexes as $index) {
  $helpers->create_index($index);
}
