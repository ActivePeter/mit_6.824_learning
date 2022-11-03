$vrb = 1
while( $vrb -lt 1000){
go test -run 2B  | Out-File -Append .\1.txt
echo $vrb
$vrb=$vrb+1
}