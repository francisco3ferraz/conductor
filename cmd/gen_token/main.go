package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func main() {
	secret := flag.String("secret", "production-jwt-secret-key-safe", "JWT secret key")
	user := flag.String("user", "test-user", "User ID")
	roles := flag.String("roles", "admin", "Comma-separated roles")
	issuer := flag.String("issuer", "conductor-production", "Token issuer")
	flag.Parse()

	claims := jwt.MapClaims{
		"sub":   *user,
		"iss":   *issuer,
		"aud":   []string{"conductor-api"},
		"iat":   time.Now().Unix(),
		"exp":   time.Now().Add(24 * time.Hour).Unix(),
		"roles": strings.Split(*roles, ","),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(*secret))
	if err != nil {
		log.Fatalf("Failed to sign token: %v", err)
	}

	fmt.Println(tokenString)

	// Verify immediately to check for issues
	parsed, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(*secret), nil
	})

	if err != nil {
		log.Printf("ERROR: Generated token failed validation: %v", err)
	} else if parsed.Valid {
		log.Printf("SUCCESS: Generated token is valid")
	}
}
