// pagination.go
package dbutils

import (
	"math"
	"strconv"

	log "github.com/sirupsen/logrus"
)

// Paginator is the structure representing the paginator object
type Paginator struct {
	PageCount    int64
	PageSize     int64 // the limit
	Total        int64
	CurrentPage  int64
	NextPage     int64
	PreviousPage int64
	Offset       int64
	PageExists   bool
	Paging       bool
}

// HasNext returns true if there is a next page
func (p *Paginator) HasNext() bool {
	if p.HasPage(p.CurrentPage + 1) {
		return true
	}
	return false
}

// HasPrev returns true if there is a previous page
func (p *Paginator) HasPrev() bool {
	if p.HasPage(p.CurrentPage-1) && (p.CurrentPage-1) != 0 {
		return true
	}
	return false
}

// Pages returns the number of pages
func (p *Paginator) Pages() int64 {
	return int64(math.Ceil(
		(float64(p.Total) / float64(p.PageSize))))
}

// HasPages returns true if there are pages
func (p *Paginator) HasPages() bool {
	if p.Total < 1 {
		return false
	}
	return true
}

// HasPage returns true if @param page is available
func (p *Paginator) HasPage(page int64) bool {
	if p.HasPages() && page <= p.Pages() {
		return true
	}
	return false
}

// FirstItem returns the number for first item in the page - used as OFFSET
func (p *Paginator) FirstItem() int64 {
	if p.PageCount < p.CurrentPage {
		return p.Total + 1
	}
	return int64(math.Min(float64((p.CurrentPage-1)*p.PageSize+1), float64(p.Total)))
}

// LastItem returns the number for the last item in the page
func (p *Paginator) LastItem() int64 {
	return int64(math.Min(float64(p.FirstItem()+p.PageSize-1), float64(p.Total)))
}

// GetPaginator returns the a pointer to the Paginator structure
func GetPaginator(totalRecords int64, pageSize string, page string, paging bool) Paginator {
	p := Paginator{}
	p.Paging = true
	p.Total = totalRecords

	ps, err := strconv.ParseInt(pageSize, 10, 64)
	if err != nil {
		log.WithError(err).Info("Failed to convert pageSize to integer. Defaulting to 50")
		p.PageSize = 50
	} else {
		p.PageSize = ps
	}

	pc, err := strconv.ParseInt(page, 10, 64)
	if err != nil {
		log.WithError(err).Info("Failed to convert page to integer. Defaulting to 1")
		p.CurrentPage = 1
	} else {
		p.CurrentPage = pc
	}
	p.PageCount = p.Pages()
	p.Offset = p.FirstItem() - 1
	if p.HasPrev() {
		p.PreviousPage = p.CurrentPage - 1
	}
	if p.HasNext() {
		p.NextPage = p.CurrentPage + 1
	}
	p.PageExists = p.HasPage(pc)
	return p
}
