param(
    [string]$RepoPath = "C:\Users\stree\Documents\GIT_OZON"
)

Write-Host ""
Write-Host "========================================================================"
Write-Host "AUTO COMMIT AND PUSH"
Write-Host "========================================================================"
Write-Host ""

Write-Host "Repo: $RepoPath"
Write-Host ""

if (-not (Test-Path $RepoPath)) {
    Write-Host "ERROR: Folder not found: $RepoPath"
    exit 1
}

try {
    cd $RepoPath
    Write-Host "Checking git status..."
    Write-Host ""
    
    git status
    Write-Host ""
    
    Write-Host "Adding files..."
    git add .
    
    $status = git status --porcelain
    if ([string]::IsNullOrWhiteSpace($status)) {
        Write-Host "No changes to commit"
        exit 0
    }
    
    Write-Host "Committing..."
    git commit -m "Update: automatic commit"
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Commit failed"
        exit 1
    }
    
    Write-Host "Pushing to GitHub..."
    git push origin main
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Push failed"
        exit 1
    }
    
    Write-Host ""
    Write-Host "========================================================================"
    Write-Host "SUCCESS: Files uploaded to GitHub!"
    Write-Host "========================================================================"
    Write-Host ""
    Write-Host "Visit: https://github.com/streetunions-commits/OZON"
    Write-Host "Press F5 in browser to see updates"
    Write-Host ""
    
} catch {
    Write-Host "ERROR: $_"
    exit 1
}
