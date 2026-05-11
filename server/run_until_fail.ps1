$iteration = 0
while ($true) {
    $iteration++
    clear
    Write-Host "--- Iteration $iteration ---"
    cargo test --test overall_1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Test overall_1 failed on iteration $iteration. Exiting."
        exit $LASTEXITCODE
    }
    cargo test --test overall_2
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Test overall_2 failed on iteration $iteration. Exiting."
        exit $LASTEXITCODE
    }
}
