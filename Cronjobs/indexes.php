<?php
require_once '../autoload.php';

$helpers = new Helpers('closed');

$indexes=array('index_closed_36_2022','index_closed_36_2021','index_closed_36_2020','index_closed_36_2019');

foreach ($indexes as $index) {
  $helpers->create_index($index);
}
