param([string]$proc)
$q = "SELECT Name,ContextSwitchesPersec FROM Win32_PerfRawData_PerfProc_Thread WHERE Name LIKE '$proc/%'"
function Sum-CS($query) {
    (Get-CimInstance -Query $query |
        Where-Object { $_.Name -notlike '*/_Total' } |
        Measure-Object -Property ContextSwitchesPersec -Sum).Sum
}
$c0 = Sum-CS $q
$t0 = Get-Date
Start-Sleep -Seconds 3
$c1 = Sum-CS $q
$t1 = Get-Date
$p = Get-Process -Name $proc -ErrorAction SilentlyContinue
$tc = if ($p) { ($p | ForEach-Object { $_.Threads.Count } | Measure-Object -Sum).Sum } else { 0 }
$secs = ($t1 - $t0).TotalSeconds
$rate = ($c1 - $c0) / $secs
"threads={0}  ctxsw_per_sec={1:N0}  (delta over {2:N2}s)" -f $tc, $rate, $secs
