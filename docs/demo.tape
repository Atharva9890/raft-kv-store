Output docs/demo.gif
Set Shell "zsh"
Set FontSize 18
Set Width 1200
Set Height 720
Set Theme "Dracula"
Set TypingSpeed 40ms
Set Padding 20

Type "cd raft-kv-store"
Enter
Sleep 500ms

Type "docker compose up -d"
Enter
Sleep 4s

Type "docker compose logs | grep role=LEADER"
Enter
Sleep 2s

Type "go run ./cmd/kvctl put foo bar"
Enter
Sleep 1500ms
Type "go run ./cmd/kvctl get foo"
Enter
Sleep 2s

Type "./scripts/kill_leader.sh"
Enter
Sleep 3s

Type "docker compose logs | grep role=LEADER"
Enter
Sleep 2500ms

Type "go run ./cmd/kvctl get foo"
Enter
Sleep 2500ms
