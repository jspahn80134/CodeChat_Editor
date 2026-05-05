$iteration = 0
while ($true) {
    $iteration++
    clear
    Write-Host "--- Iteration $iteration ---"
    cargo test --test overall_2
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Test failed on iteration $iteration. Exiting."
        exit $LASTEXITCODE
    }
}
