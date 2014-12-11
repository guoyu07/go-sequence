<?php

$fp = fsockopen("127.0.0.1", 6381, $errno, $errstr, 30);

if (!$fp) {
    echo "$errstr ($errno)<br />\n";
} else {
    $out = '{"AppName":"test", "IdQueue":"test", "step":10}';
    $out .= "\n";
    fwrite($fp, $out);
    // echo fgets($fp, 128);
    echo stream_get_line($fp, 128, "\n");
    fclose($fp);
}

