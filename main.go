package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Command Line Flags
var (
	logLevel  = flag.String("loglevel", "info", "Set log level: debug, info, warn, error")
	inputFile = flag.String("file", "./data/sudoku.many.17clue.txt", "Path to the Sudoku input file")
	logDir    = flag.String("logdir", "./logs", "Directory for CSV log files")
)

var logger *slog.Logger

const (
	boardSize = 81
	rowLength = 9
	colLength = 9
)

type Board struct {
	Cells   [boardSize]int // Initial puzzle clues (0 for empty)
	Guesses [boardSize]int // Guesses made by the solver (0 for empty)
}

type Guess struct {
	Row, Col, Value int
}

// SolverState holds the board and the cached used numbers for rows, cols, squares.
type SolverState struct {
	Board *Board

	RowsUsed    [rowLength]uint16
	ColsUsed    [colLength]uint16
	SquaresUsed [(rowLength / 3) * (colLength / 3)]uint16
}

// NewSolverState initializes the state including pre-populating used numbers from clues.
func NewSolverState(board *Board) *SolverState {
	// Arrays are zero-initialized, which is correct for empty masks
	state := &SolverState{Board: board}

	// Pre-populate based on initial clues
	for r := range 9 {
		for c := range 9 {
			idx := r*rowLength + c
			num := board.Cells[idx]
			if num != 0 {
				squareIdx := (r/3)*3 + (c / 3)
				bit := uint16(1 << num) // The bit corresponding to the number

				// Check for initial board validity using bitmasks
				if (state.RowsUsed[r]&bit) != 0 || (state.ColsUsed[c]&bit) != 0 || (state.SquaresUsed[squareIdx]&bit) != 0 {
					logger.Warn("Initial board invalid: duplicate number found",
						"number", num,
						"row", r,
						"col", c,
					)
				}
				// Set the corresponding bit in the masks
				state.RowsUsed[r] |= bit
				state.ColsUsed[c] |= bit
				state.SquaresUsed[squareIdx] |= bit
			}
		}
	}
	return state
}

// IsValid checks the pre-computed state if a guess is valid.
func (ss *SolverState) IsValid(g Guess) bool {
	if g.Value < 1 || g.Value > 9 {
		return false
	}
	squareIdx := (g.Row/3)*3 + (g.Col / 3)
	bit := uint16(1 << g.Value)
	// Check if the bit is NOT set in any of the masks
	return (ss.RowsUsed[g.Row]&bit) == 0 &&
		(ss.ColsUsed[g.Col]&bit) == 0 &&
		(ss.SquaresUsed[squareIdx]&bit) == 0
}

// Pretty displays the board, showing initial clues and guesses.
func (b *Board) Pretty() string {
	var sb strings.Builder
	sb.WriteString("+-------+-------+-------+\n")
	for r := range rowLength {
		sb.WriteString("| ")
		for c := range colLength {
			idx := r*rowLength + c
			val := 0
			if b.Cells[idx] != 0 {
				val = b.Cells[idx]
			} else if b.Guesses[idx] != 0 {
				val = b.Guesses[idx]
			}

			if val == 0 {
				sb.WriteString(". ")
			} else {
				sb.WriteString(fmt.Sprintf("%d ", val))
			}
			if (c+1)%3 == 0 {
				sb.WriteString("| ")
			}
		}
		sb.WriteString("\n")
		if (r+1)%3 == 0 && r < rowLength-1 {
			sb.WriteString("+-------+-------+-------+\n")
		}
	}
	sb.WriteString("+-------+-------+-------+\n")
	return sb.String()
}

// Deserialize reads Sudoku puzzles from a file (one per line).
func Deserialize(path string) ([]Board, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		// Return error to be logged by caller
		return nil, fmt.Errorf("reading file %s: %w", path, err)
	}

	content := strings.ReplaceAll(string(bytes), "\r\n", "\n")
	lines := strings.Split(content, "\n")
	boards := make([]Board, 0, len(lines))

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		if len(line) != boardSize {
			logger.Warn("Skipping line: incorrect length",
				"line_number", i+1,
				"expected_length", boardSize,
				"actual_length", len(line),
			)
			continue
		}

		var cells [boardSize]int
		validLine := true
		for j, char := range line {
			if char >= '1' && char <= '9' {
				cells[j] = int(char - '0')
			} else if char == '0' || char == '.' {
				cells[j] = 0
			} else {
				logger.Warn("Skipping line: invalid character",
					"line_number", i+1,
					"position", j,
					"character", string(char),
				)
				validLine = false
				break
			}
		}

		if validLine {
			boards = append(boards, Board{Cells: cells})
		}
	}
	// Check if boards is empty only if there were non-empty lines processed
	if len(boards) == 0 {
		nonEmptyLines := false
		for _, line := range lines {
			if strings.TrimSpace(line) != "" {
				nonEmptyLines = true
				break
			}
		}
		if nonEmptyLines {
			return nil, fmt.Errorf("no valid boards found in file %s", path)
		}
	}

	return boards, nil
}

func SolveBacktrack(state *SolverState, startIdx int) bool {
	var emptyIdx = -1
	for i := startIdx; i < boardSize; i++ {
		if state.Board.Cells[i] == 0 && state.Board.Guesses[i] == 0 {
			emptyIdx = i
			break
		}
	}

	if emptyIdx == -1 {
		return true // Solved
	}

	row := emptyIdx / rowLength
	col := emptyIdx % colLength
	squareIdx := (row/3)*3 + (col / 3)

	for num := 1; num <= 9; num++ {
		bit := uint16(1 << num) // Calculate bit for the current number

		// If result is 0, the bit is not set
		if (state.RowsUsed[row]&bit) == 0 &&
			(state.ColsUsed[col]&bit) == 0 &&
			(state.SquaresUsed[squareIdx]&bit) == 0 {

			state.Board.Guesses[emptyIdx] = num // Update boards guess

			// Update masks using bitwise OR to set the bit
			state.RowsUsed[row] |= bit
			state.ColsUsed[col] |= bit
			state.SquaresUsed[squareIdx] |= bit

			if SolveBacktrack(state, emptyIdx+1) {
				return true
			}

			state.Board.Guesses[emptyIdx] = 0 // Clear boards guess

			// Revert masks using bitwise AND NOT which clears the bit:
			state.RowsUsed[row] &= ^bit
			state.ColsUsed[col] &= ^bit
			state.SquaresUsed[squareIdx] &= ^bit
		}
	}
	return false // Backtrack
}

// setupCSVLogger creates the log directory, generates a filename, opens the file,
// creates a csv.Writer, and writes the header. (No changes needed)
func setupCSVLogger(logDir string) (*csv.Writer, *os.File, error) {
	err := os.MkdirAll(logDir, 0755)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create log directory '%s': %w", logDir, err)
	}
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join(logDir, fmt.Sprintf("sudoku_log_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create CSV log file '%s': %w", filename, err)
	}
	logger.Info("CSV log file created", "path", filename)
	writer := csv.NewWriter(file)
	header := []string{"BoardIndex", "Status", "StartTime", "EndTime", "DurationSeconds"}
	if err := writer.Write(header); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to write CSV header to '%s': %w", filename, err)
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("error flushing CSV header to '%s': %w", filename, err)
	}
	return writer, file, nil
}

// csvLoggerGoroutine reads rows from a channel and writes them to the CSV file immediately.
func csvLoggerGoroutine(csvWriter *csv.Writer, logChan <-chan []string, wg *sync.WaitGroup) {
	defer wg.Done()

	logger.Debug("CSV logger goroutine started")
	for row := range logChan {
		if err := csvWriter.Write(row); err != nil {
			logger.Error("CSV goroutine: failed to write row", "error", err, "row_data", strings.Join(row, ","))
		}
		// Flush after every write to ensure data is written immediately
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			logger.Error("CSV goroutine: failed to flush writer", "error", err)
		}
	}
	logger.Debug("CSV logger goroutine finished")
}

func main() {
	flag.Parse()

	// slog Setup
	var programLevel = new(slog.LevelVar)
	switch strings.ToLower(*logLevel) {
	case "debug":
		programLevel.Set(slog.LevelDebug)
	case "info":
		programLevel.Set(slog.LevelInfo)
	case "warn", "warning":
		programLevel.Set(slog.LevelWarn)
	case "error":
		programLevel.Set(slog.LevelError)
	default:
		fmt.Fprintf(os.Stderr, "Unknown log level '%s', defaulting to INFO\n", *logLevel)
	}
	handlerOpts := slog.HandlerOptions{Level: programLevel}
	handler := slog.NewTextHandler(os.Stdout, &handlerOpts)
	logger = slog.New(handler)

	// CSV Logger Setup
	csvWriter, csvFile, err := setupCSVLogger(*logDir)
	if err != nil {
		logger.Error("Failed to set up CSV logger", "error", err)
		os.Exit(1)
	}

	// Channel and WaitGroup for CSV Goroutine
	csvLogChan := make(chan []string, 100)
	var csvWg sync.WaitGroup

	// Start the logger goroutine
	csvWg.Add(1)
	go csvLoggerGoroutine(csvWriter, csvLogChan, &csvWg)

	// Ensure file is closed and goroutine finishes properly on exit
	defer func() {
		logger.Debug("Main: Closing CSV log channel")
		close(csvLogChan) // Signal the logger goroutine to exit after processing remaining items

		logger.Debug("Main: Waiting for CSV logger goroutine to finish")
		csvWg.Wait() // Wait for the logger goroutine to complete
		logger.Debug("Main: CSV logger goroutine finished")

		// Now it's safe to close the file
		if csvFile != nil {
			logger.Debug("Main: Closing CSV file")
			if err := csvFile.Close(); err != nil {
				logger.Error("Error closing CSV file on exit", "error", err)
			}
		}
	}()

	overallStartTime := time.Now()
	logger.Info("Starting Sudoku solver", "log_level", programLevel.Level(), "file", *inputFile, "csv_log_dir", *logDir)

	boards, err := Deserialize(*inputFile)
	if err != nil {
		logger.Error("Failed to deserialize boards", "error", err)
		os.Exit(1)
	}

	if len(boards) == 0 {
		logger.Info("No valid Sudoku boards found in the file.")
		os.Exit(0)
	}

	logger.Info("Deserialized boards", "count", len(boards))

	solvedCount := 0
	// Solving Loop
	for i := range boards {
		boardStartTime := time.Now()
		currentBoard := &boards[i]

		logger.Info("Attempting board", "index", i+1)

		if logger.Enabled(context.Background(), slog.LevelDebug) {
			logger.Debug("Printing initial board state")
			fmt.Println("Initial Board:")
			fmt.Println(currentBoard.Pretty())
		}

		initState := NewSolverState(currentBoard)
		solved := SolveBacktrack(initState, 0)
		boardEndTime := time.Now()
		boardDuration := boardEndTime.Sub(boardStartTime)

		status := "Failed"
		if solved {
			solvedCount++
			status = "Solved"
			logger.Info("Successfully solved board", "index", i+1, "duration", boardDuration.String())
			if logger.Enabled(context.Background(), slog.LevelDebug) {
				logger.Debug("Printing solved board state")
				fmt.Println("Solved Board:")
				fmt.Println(currentBoard.Pretty())
			}
		} else {
			logger.Warn("Failed to solve board", "index", i+1, "duration", boardDuration.String())
			if logger.Enabled(context.Background(), slog.LevelDebug) {
				logger.Debug("Printing board state at failure")
				fmt.Println("Board state at failure:")
				fmt.Println(currentBoard.Pretty())
			}
		}

		// Send data to CSV logger goroutine
		csvRow := []string{
			strconv.Itoa(i + 1),
			status,
			boardStartTime.Format(time.RFC3339),
			boardEndTime.Format(time.RFC3339),
			fmt.Sprintf("%.6f", boardDuration.Seconds()),
		}
		// Send the row to the channel
		csvLogChan <- csvRow
	}

	overallTime := time.Since(overallStartTime)
	logger.Info("Finished processing all boards",
		"solved_count", solvedCount,
		"total_boards", len(boards),
		"total_duration", overallTime.String(),
	)
}
