package kafka

import (
	"fmt"
)

// validateTopic rejects empty topics, names over Kafka's 249-char limit, and
// names containing non-printable-ASCII characters.
func (k *Client) validateTopic(topic string) error {
	if topic == "" {
		return fmt.Errorf("topic name cannot be empty")
	}

	if len(topic) > 249 {
		return fmt.Errorf("topic name too long, maximum length is 249 characters")
	}

	for _, char := range topic {
		if char < 32 || char > 126 {
			return fmt.Errorf("topic name contains invalid characters")
		}
	}

	return nil
}
