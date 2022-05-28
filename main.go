package crawler

import (
	"bytes"
	"github.com/PuerkitoBio/goquery"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

func Run(u string, timeout time.Duration, f CheckPageContentFunc, statusCodeMin, statusCodeMax int, onlySameHost bool, linksLimit int) error {
	if err := parseRecursive(u, timeout, statusCodeMin, statusCodeMax, f, onlySameHost, linksLimit); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

var (
	requested = make(map[string]struct{})
	requestedMtx sync.RWMutex
)

type CheckPageContentFunc func(string)error

func parseRecursive(u string, timeout time.Duration, statusCodeMin, statusCodeMax int, f CheckPageContentFunc, onlySameHost bool, linksLimit int) error {
	lf := logrus.WithField("parentUrl", u).WithField("linksLimit", linksLimit)
	ll, err := parse(u, timeout, statusCodeMin, statusCodeMax, f, onlySameHost)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, l := range ll {
		if enough := func() bool {
			if linksLimit == 0 {
				return false
			}
			requestedMtx.RLock()
			defer requestedMtx.RUnlock()
			lf = lf.WithField("len(requested)", len(requested))
			return len(requested) > linksLimit
		}(); enough {
			lf.Infof("this is enough links")
			return nil
		}
		if errP := parseRecursive(l, timeout, statusCodeMin, statusCodeMax, f, onlySameHost, linksLimit); errP != nil {
			return errors.WithStack(errP)
		}
	}
	return nil
}

func parse(u string, timeout time.Duration, statusCodeMin, statusCodeMax int, f CheckPageContentFunc, onlySameHost bool) (links []string, finalErr error) {
	l := logrus.WithField("u", u)
	t1 := time.Now()
	defer func() {
		if finalErr != nil {
			finalErr = errors.Wrapf(finalErr, "for url %+v", u)
		}
		l.Infof("requested for %+v", time.Since(t1))
	}()
	isRequested := func() bool {
		requestedMtx.RLock()
		defer requestedMtx.RUnlock()
		l = l.WithField("len(requested)", len(requested))
		_, exists := requested[u]
		return exists
	}()
	if isRequested {
		l.Infof("it's already requested, skip it")
		return nil, nil
	}
	l.Infof("requesting...")
	cl := http.Client{Timeout: timeout}
	resp, err := cl.Get(u)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.StatusCode < statusCodeMin || resp.StatusCode > statusCodeMax {
		return nil, errors.Errorf("bad status code: %+v, supported status code range: %+v - %+v", resp.StatusCode, statusCodeMin, statusCodeMax)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if errF := f(string(b)); errF != nil {
		return nil, errors.WithStack(errF)
	}
	r := bytes.NewReader(b)
	d, err := goquery.NewDocumentFromReader(r)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	func(){
		requestedMtx.Lock()
		defer requestedMtx.Unlock()
		requested[u] = struct{}{}
	}()

	up, err := url.Parse(u)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	aa := d.Find("a")
	done := make(chan struct{})
	errs := make(chan error, aa.Length())
	go func(){
		aa.Each(func(i int, s *goquery.Selection) {
			href, exists := s.Attr("href")
			if !exists {
				return
			}
			href = strings.TrimSpace(href)
			if len(href) == 0 {
				return
			}
			up2, err2 := url.Parse(href)
			if err2 != nil {
				errs <- err2
				return
			}
			if onlySameHost {
				if len(up2.Host) != 0 && !sameMainDomain(up.Host, up2.Host) {
					return
				}
			}
			if up2.Host == "" {
				up2.Host = up.Host
			}
			if len(up2.Scheme) == 0 {
				up2.Scheme = up.Scheme
			}
			links = append(links, up2.String())
		})
		done <- struct{}{}
	}()
	select {
	case <-done:
	case errCh := <-errs:
		return nil, errors.WithStack(errCh)
	}
	return links, nil
}

// Converts sub.domain.com to domain.com
// and sub2.sub.domain.com to domain.com
func mainDomain(host string) string {
	const (
		subDomainSep = "."
		mainDomainParts = 2
	)
	pp := strings.Split(host, subDomainSep)
	if len(pp) <= mainDomainParts {
		return host
	}
	return strings.Join(pp[len(pp)-mainDomainParts:], subDomainSep)
}

func sameMainDomain(host1, host2 string) bool {
	return mainDomain(host1) == mainDomain(host2)
}