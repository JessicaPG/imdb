package com.imdb.controller;

import com.imdb.dao.MovieDao;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Resource;
import java.util.List;


@Controller
public class HomeController {
    @Resource
    private MovieDao movieDao;



    @GetMapping("/")
    public String home() {
        return "Home";
    }

    @GetMapping("/TypecastingView")
    public String button1() {
        return "TypecastingView";
    }


    @GetMapping("/CoincidenceView")
    public String button2() {
        return "CoincidenceView";
    }

    @GetMapping("/SixDegreesView")
    public String button3() {
        return "SixDegreesView";
    }

    @GetMapping("/TypecastingResultsView")
    public String typeCastingQuery(@RequestParam(name = "name", required = false) String name, Model model) throws Exception {
        boolean isTyp = movieDao.isTypecasting(name);
        model.addAttribute("resultTyp", isTyp);
        ;
        return "TypecastingResultsView";
    }

    @GetMapping("/CoincidenceViewResults")
    public String coincidenceQuery(@RequestParam(name = "name1", required = false) String name1, @RequestParam(name = "name2", required = false) String name2, Model model) throws Exception {
        List<String> listMovies = movieDao.findCoincidence(name1, name2);
        model.addAttribute("listMovies", listMovies);
        return "CoincidenceResultsView";
    }

    @GetMapping("/SixDegreesResultsView")
    public String sixDegreesQuery(@RequestParam(name = "name", required = false) String name, Model model) {
        int degrees = movieDao.sixDegrees(name);
        model.addAttribute("degrees", degrees);
        return "SixDegreesResultsView";

    }
}

