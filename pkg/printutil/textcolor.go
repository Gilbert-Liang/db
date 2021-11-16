package printutil

import "fmt"

type Color int

const (
	Black Color = iota + 30
	Red
	Green
	Yellow
	Blue
	Purple
	Cyan
	White
)

func BlackText(str string) string {
	return textColor(Black, str)
}

func RedText(str string) string {
	return textColor(Red, str)
}
func YellowText(str string) string {
	return textColor(Yellow, str)
}
func GreenText(str string) string {
	return textColor(Green, str)
}
func CyanText(str string) string {
	return textColor(Cyan, str)
}
func BlueText(str string) string {
	return textColor(Blue, str)
}
func PurpleText(str string) string {
	return textColor(Purple, str)
}
func WhiteText(str string) string {
	return textColor(White, str)
}

func textColor(color Color, str string) string {
	return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", color, str)
}
