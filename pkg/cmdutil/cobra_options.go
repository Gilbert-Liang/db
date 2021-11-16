package cmdutil

import "github.com/spf13/cobra"

func GetDefaultCompletionOptions() cobra.CompletionOptions {
	return cobra.CompletionOptions{
		DisableDefaultCmd:   true,
		DisableDescriptions: true,
		DisableNoDescFlag:   true,
	}
}
