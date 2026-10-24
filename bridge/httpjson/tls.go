package httpjson

import "crypto/tls"

// Keep the transport policy even when an application selects a per-client
// configuration. Clone callback results rather than modifying shared configs.
func transportTLS(config *tls.Config) *tls.Config {
	if config == nil {
		config = &tls.Config{}
	}
	secured := config.Clone()
	if config.RootCAs != nil {
		secured.RootCAs = config.RootCAs.Clone()
	}
	if config.ClientCAs != nil {
		secured.ClientCAs = config.ClientCAs.Clone()
	}
	secured.Certificates = make([]tls.Certificate, len(config.Certificates))
	for i, certificate := range config.Certificates {
		secured.Certificates[i] = copyCertificate(certificate)
	}
	if config.NameToCertificate != nil {
		secured.NameToCertificate = make(map[string]*tls.Certificate, len(config.NameToCertificate))
		for name, certificate := range config.NameToCertificate {
			if certificate != nil {
				copied := copyCertificate(*certificate)
				secured.NameToCertificate[name] = &copied
			}
		}
	}
	secured.CipherSuites = append([]uint16(nil), config.CipherSuites...)
	secured.CurvePreferences = append([]tls.CurveID(nil), config.CurvePreferences...)
	if secured.MinVersion < tls.VersionTLS12 {
		secured.MinVersion = tls.VersionTLS12
	}
	secured.NextProtos = []string{"http/1.1"}
	if selectConfig := secured.GetConfigForClient; selectConfig != nil {
		secured.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			selected, err := selectConfig(hello)
			if err != nil || selected == nil {
				return selected, err
			}
			return transportTLS(selected), nil
		}
	}
	return secured
}

func copyCertificate(certificate tls.Certificate) tls.Certificate {
	certificate.Certificate = copyBytes(certificate.Certificate)
	certificate.OCSPStaple = append([]byte(nil), certificate.OCSPStaple...)
	certificate.SignedCertificateTimestamps = copyBytes(certificate.SignedCertificateTimestamps)
	certificate.SupportedSignatureAlgorithms = append([]tls.SignatureScheme(nil), certificate.SupportedSignatureAlgorithms...)
	// Leaf is only a parsed cache; let TLS parse the owned certificate bytes.
	certificate.Leaf = nil
	return certificate
}

func copyBytes(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for i, value := range values {
		result[i] = append([]byte(nil), value...)
	}
	return result
}
